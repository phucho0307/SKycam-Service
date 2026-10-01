-- Users and per-device authorization.
--
-- The service never creates users from a request: `user_id` is the JWT `sub`
-- claim (a Google subject id in the real system), minted by the `api` crate.
-- This table decides what that subject may *do*, which is a separate question
-- from who they are.

CREATE TABLE users (
    user_id      text PRIMARY KEY,
    email        text        NOT NULL,
    display_name text,
    -- A platform-wide escape hatch, kept deliberately small: it grants every
    -- device. Per-device access belongs in device_grants, not here.
    is_admin     boolean     NOT NULL DEFAULT false,
    -- Soft delete. Revoking access must not orphan audit rows that name the
    -- user, and a disabled user has to stay resolvable for that reason.
    disabled_at  timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT user_id_charset CHECK (user_id ~ '^[A-Za-z0-9_.:@|-]{1,128}$')
);

-- Case-insensitive: nobody should be able to register Alice@x and alice@x.
CREATE UNIQUE INDEX users_email_idx ON users (lower(email));

-- Which devices a user may see or drive, and in what capacity.
--   viewer   -- read frames and telemetry
--   operator -- viewer, plus change settings and send commands
--   admin    -- operator, plus manage grants on that device
CREATE TABLE device_grants (
    user_id    text        NOT NULL REFERENCES users (user_id)     ON DELETE CASCADE,
    device_id  text        NOT NULL REFERENCES devices (device_id) ON DELETE CASCADE,
    role       text        NOT NULL CHECK (role IN ('viewer', 'operator', 'admin')),
    granted_at timestamptz NOT NULL DEFAULT now(),
    granted_by text,
    -- The composite primary key IS the hot-path index: every authorized call
    -- asks exactly "what is (this user, this device)?", which this answers with
    -- a single index lookup and no separate index to maintain.
    PRIMARY KEY (user_id, device_id)
);

-- The other direction: "who can touch this camera?", for an admin screen and
-- for revoking access when a device changes hands.
CREATE INDEX device_grants_device_idx ON device_grants (device_id);
