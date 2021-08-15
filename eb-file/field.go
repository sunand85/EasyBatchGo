package eb_file

import "strconv"

type Field struct {
	index      int
	rawContent string
}

func NewField(index int, rawContent string) *Field {
	return &Field{index: index, rawContent: rawContent}
}

func (f Field) toString() string {
	return "Field: { index=" + strconv.Itoa(f.index) + ", rawContent='" + f.rawContent + "'}"
}
