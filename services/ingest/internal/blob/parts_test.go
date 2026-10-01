package blob

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func part(n int32, size int64) types.Part {
	return types.Part{PartNumber: aws.Int32(n), Size: aws.Int64(size), ETag: aws.String("e")}
}

func TestContiguous(t *testing.T) {
	const ps = 100
	tests := []struct {
		name          string
		parts         []types.Part
		total         int64
		wantParts     int
		wantCommitted int64
	}{
		{"no parts", nil, 250, 0, 0},
		{"two full parts", []types.Part{part(1, ps), part(2, ps)}, 250, 2, 200},
		{"out of order input", []types.Part{part(2, ps), part(1, ps)}, 250, 2, 200},
		{"gap stops the run", []types.Part{part(1, ps), part(3, ps)}, 350, 1, 100},
		{"missing part 1", []types.Part{part(2, ps)}, 250, 0, 0},
		{"short final part that ends the file", []types.Part{part(1, ps), part(2, ps), part(3, 50)}, 250, 3, 250},
		{"short part that doesn't end the file", []types.Part{part(1, ps), part(2, 50)}, 350, 1, 100},
		// A short part 1 in a longer file is invalid (only the last part may be
		// short), so nothing is trusted and the upload restarts at 0.
		{"short part mid-run is not final", []types.Part{part(1, 50), part(2, ps)}, 150, 0, 0},
		{"unknown total accepts short final", []types.Part{part(1, ps), part(2, 30)}, -1, 2, 130},
		{"empty part is never valid", []types.Part{part(1, 0)}, -1, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keep, committed := contiguous(tt.parts, ps, tt.total)
			if len(keep) != tt.wantParts || committed != tt.wantCommitted {
				t.Fatalf("got %d parts / %d bytes, want %d parts / %d bytes",
					len(keep), committed, tt.wantParts, tt.wantCommitted)
			}
		})
	}
}
