package blob

import (
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// PartSize is the fixed multipart part size. S3 requires every part except the
// last to be at least 5MiB, and a fixed size makes offsets map to part numbers
// exactly: part n covers bytes [(n-1)*PartSize, n*PartSize).
const PartSize = 5 << 20

// contiguous returns the parts that can be trusted for a resume, and how many
// bytes they cover. It walks parts in order from part 1 and stops at the first
// gap. A short part is only valid as the final part of the file, so it ends the
// run; with total < 0 (size unknown) any short part is accepted as final.
//
// Anything after the returned run is ignored and will be overwritten, since
// uploading a part number again replaces it.
func contiguous(parts []types.Part, partSize, total int64) ([]types.CompletedPart, int64) {
	sorted := append([]types.Part(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool {
		return aws.ToInt32(sorted[i].PartNumber) < aws.ToInt32(sorted[j].PartNumber)
	})

	var keep []types.CompletedPart
	var committed int64
	for i, p := range sorted {
		if aws.ToInt32(p.PartNumber) != int32(i+1) {
			break // gap
		}
		size := aws.ToInt64(p.Size)
		full := size == partSize
		final := size > 0 && size < partSize && (total < 0 || committed+size == total)
		if !full && !final {
			break
		}
		keep = append(keep, types.CompletedPart{ETag: p.ETag, PartNumber: p.PartNumber})
		committed += size
		if final {
			break
		}
	}
	return keep, committed
}
