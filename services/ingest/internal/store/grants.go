package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Role is what a user may do with one device. Ordered: each includes the one
// before it.
type Role string

const (
	RoleViewer   Role = "viewer"
	RoleOperator Role = "operator"
	RoleAdmin    Role = "admin"
)

var roleRank = map[Role]int{RoleViewer: 1, RoleOperator: 2, RoleAdmin: 3}

// AtLeast reports whether r satisfies the required role.
func (r Role) AtLeast(required Role) bool {
	return roleRank[r] >= roleRank[required] && roleRank[r] > 0
}

func (r Role) Valid() bool { return roleRank[r] > 0 }

var (
	// ErrUnknownUser means the token verified but names nobody we know. A valid
	// signature is authentication; it is not authorization.
	ErrUnknownUser = errors.New("user is not provisioned")
	// ErrUserDisabled means the account exists but access was revoked.
	ErrUserDisabled = errors.New("user is disabled")
	// ErrNoGrant means the user has no access to that specific device.
	ErrNoGrant = errors.New("no grant for this device")
)

// Access is the answer to "who is this, and what may they do to this device?".
type Access struct {
	UserID  string
	IsAdmin bool
	Role    Role // empty when the user has no grant on the device
}

// Allows reports whether this access satisfies the required role. Platform
// admins pass everything; everyone else needs a grant on that device.
func (a Access) Allows(required Role) bool {
	return a.IsAdmin || a.Role.AtLeast(required)
}

// GrantStore answers authorization questions.
type GrantStore interface {
	// AccessFor resolves a user and their role on one device in a single query.
	// A missing grant is not an error: it returns Access with an empty Role, so
	// the caller decides whether that is fatal (it is for a write, not for a
	// platform admin).
	AccessFor(ctx context.Context, userID, deviceID string) (Access, error)
	// DevicesFor lists the devices a user may see, for filtering list views.
	// Platform admins get nil with allAll=true rather than every device id.
	DevicesFor(ctx context.Context, userID string) (devices map[string]Role, allAll bool, err error)
	UpsertUser(ctx context.Context, u User) error
	GrantDevice(ctx context.Context, userID, deviceID string, role Role, grantedBy string) error
	RevokeDevice(ctx context.Context, userID, deviceID string) error
}

type User struct {
	UserID      string
	Email       string
	DisplayName string
	IsAdmin     bool
}

// AccessFor is one round trip: a LEFT JOIN so a user with no grant on this
// device still resolves (and is still checked for being disabled) rather than
// looking identical to a user who does not exist.
func (p *Postgres) AccessFor(ctx context.Context, userID, deviceID string) (Access, error) {
	var (
		isAdmin  bool
		disabled bool
		role     *string
	)
	err := p.pool.QueryRow(ctx, `
		SELECT u.is_admin, u.disabled_at IS NOT NULL, g.role
		  FROM users u
		  LEFT JOIN device_grants g
		    ON g.user_id = u.user_id AND g.device_id = $2
		 WHERE u.user_id = $1`, userID, deviceID).Scan(&isAdmin, &disabled, &role)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Access{}, ErrUnknownUser
		}
		return Access{}, fmt.Errorf("lookup access: %w", err)
	}
	if disabled {
		return Access{}, ErrUserDisabled
	}
	a := Access{UserID: userID, IsAdmin: isAdmin}
	if role != nil {
		a.Role = Role(*role)
	}
	return a, nil
}

func (p *Postgres) DevicesFor(ctx context.Context, userID string) (map[string]Role, bool, error) {
	var isAdmin, disabled bool
	err := p.pool.QueryRow(ctx,
		`SELECT is_admin, disabled_at IS NOT NULL FROM users WHERE user_id = $1`, userID).
		Scan(&isAdmin, &disabled)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, ErrUnknownUser
		}
		return nil, false, fmt.Errorf("lookup user: %w", err)
	}
	if disabled {
		return nil, false, ErrUserDisabled
	}
	if isAdmin {
		return nil, true, nil
	}

	rows, err := p.pool.Query(ctx,
		`SELECT device_id, role FROM device_grants WHERE user_id = $1`, userID)
	if err != nil {
		return nil, false, fmt.Errorf("list grants: %w", err)
	}
	defer rows.Close()
	out := map[string]Role{}
	for rows.Next() {
		var id, role string
		if err := rows.Scan(&id, &role); err != nil {
			return nil, false, err
		}
		out[id] = Role(role)
	}
	return out, false, rows.Err()
}

func (p *Postgres) UpsertUser(ctx context.Context, u User) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO users (user_id, email, display_name, is_admin)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id) DO UPDATE
		   SET email = EXCLUDED.email,
		       display_name = EXCLUDED.display_name,
		       is_admin = EXCLUDED.is_admin`,
		u.UserID, u.Email, nullIfEmpty(u.DisplayName), u.IsAdmin)
	return err
}

func (p *Postgres) GrantDevice(ctx context.Context, userID, deviceID string, role Role, grantedBy string) error {
	if !role.Valid() {
		return fmt.Errorf("invalid role %q", role)
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO device_grants (user_id, device_id, role, granted_by)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, device_id) DO UPDATE
		   SET role = EXCLUDED.role,
		       granted_at = now(),
		       granted_by = EXCLUDED.granted_by`,
		userID, deviceID, string(role), nullIfEmpty(grantedBy))
	return err
}

func (p *Postgres) RevokeDevice(ctx context.Context, userID, deviceID string) error {
	_, err := p.pool.Exec(ctx,
		`DELETE FROM device_grants WHERE user_id = $1 AND device_id = $2`, userID, deviceID)
	return err
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
