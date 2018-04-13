package binary

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/line"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/tlv"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type Reader struct {
	r          io.Reader
	lineWriter *line.Writer
	readHeader bool
	pr         *PointsReader
	buf        tsm1.Values
	state      readerState
	stats      readerStats
}

type readerStats struct {
	series int
	counts [8]struct {
		series, values int
	}
}
type readerState byte

const (
	readHeader readerState = iota + 1
	readBucket
	readSeries
	readPoints
	done
)

func NewReader(reader io.Reader, lineWriter *line.Writer) *Reader {
	return &Reader{r: reader, lineWriter: lineWriter, buf: make(tsm1.Values, tsdb.DefaultMaxPointsPerBlock), state: readHeader}
}

func (r *Reader) ReadHeader() (*Header, error) {
	if r.state != readHeader {
		return nil, fmt.Errorf("expected reader in state %v, was in state %v\n", readHeader, r.state)
	}

	var magic [len(Magic)]byte
	n, err := r.r.Read(magic[:])
	if err != nil {
		return nil, err
	}

	if n < len(Magic) || !bytes.Equal(magic[:], Magic[:]) {
		return nil, errors.New("IFLXDUMP header not present")
	}

	t, lv, err := tlv.ReadTLV(r.r)
	if err != nil {
		return nil, err
	}
	if t != byte(HeaderType) {
		return nil, fmt.Errorf("expected header type, got %v", t)
	}
	h := &Header{}
	err = h.Unmarshal(lv)
	r.state = readBucket

	return h, err
}

func (r *Reader) Close() error {
	return nil
}

func (r *Reader) NextBucket() (*BucketHeader, error) {
	if r.state != readBucket {
		return nil, fmt.Errorf("expected reader in state %v, was in state %v", readBucket, r.state)
	}

	t, lv, err := tlv.ReadTLV(r.r)
	if err != nil {
		if err == io.EOF {
			r.state = done
			return nil, nil
		}
		return nil, err
	}
	if t != byte(BucketHeaderType) {
		return nil, fmt.Errorf("expected bucket header type, got %v", t)
	}

	bh := &BucketHeader{}
	err = bh.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	r.state = readSeries

	return bh, nil
}

func (r *Reader) NextSeries() (*SeriesHeader, error) {
	if r.state != readSeries {
		return nil, fmt.Errorf("expected reader in state %v, was in state %v", readSeries, r.state)
	}

	t, lv, err := tlv.ReadTLV(r.r)
	if t == byte(BucketFooterType) {
		r.state = readBucket
		return nil, nil
	}
	if t != byte(SeriesHeaderType) {
		return nil, fmt.Errorf("expected series header type, got %v", t)
	}
	sh := &SeriesHeader{}
	err = sh.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	r.stats.series++

	var pointsType MessageType
	switch sh.FieldType {
	case FloatFieldType:
		pointsType = FloatPointsType
	case IntegerFieldType:
		pointsType = IntegerPointsType
	case UnsignedFieldType:
		pointsType = UnsignedPointsType
	case BooleanFieldType:
		pointsType = BooleanPointsType
	case StringFieldType:
		pointsType = StringPointsType
	default:
		return nil, fmt.Errorf("unsupported series field type %v", sh.FieldType)
	}

	r.state = readPoints
	r.pr = &PointsReader{pointsType: pointsType, r: r.r, values: r.buf, state: &r.state, stats: &r.stats}
	return sh, nil
}

func (r *Reader) Points() *PointsReader {
	return r.pr
}

type PointsReader struct {
	pointsType MessageType
	r          io.Reader
	values     tsm1.Values
	n          int
	state      *readerState
	stats      *readerStats
	lineWriter *line.Writer
}

func (pr *PointsReader) Next() (bool, error) {
	if *pr.state != readPoints {
		return false, fmt.Errorf("expected reader in state %v, was in state %v", readPoints, *pr.state)
	}

	t, lv, err := tlv.ReadTLV(pr.r)
	if err != nil {
		return false, err
	}
	if t == byte(SeriesFooterType) {
		*pr.state = readSeries
		return false, nil
	}
	if t != byte(pr.pointsType) {
		if pr.lineWriter != nil {
			err = pr.writeLineProtocol(MessageType(t), lv)
			if err != nil {
				return false, err
			} else {
				return false, nil
			}
		} else {
			return false, fmt.Errorf("expected message type %v, got %v", pr.pointsType, MessageType(t))
		}
	}
	err = pr.marshalValues(pr.pointsType, lv)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (pr *PointsReader) writeLineProtocol(pointsType MessageType, lv []byte) error {
	err := pr.marshalValues(pointsType, lv)
	if err != nil {
		return err
	}

	var c tsdb.Cursor
	switch pointsType {
	case FloatPointsType:
		fp, err := pr.marshalFloats(lv)
		if err != nil {
			return err
		}
		c = &floatSingleBatchCursor{keys: fp.Timestamps, values: fp.Values}
	case IntegerPointsType:
		ip, err := pr.marshalIntegers(lv)
		if err != nil {
			return err
		}
		c = &integerSingleBatchCursor{keys: ip.Timestamps, values: ip.Values}
	case UnsignedPointsType:
		up, err := pr.marshalUnsigned(lv)
		if err != nil {
			return err
		}
		c = &unsignedSingleBatchCursor{keys: up.Timestamps, values: up.Values}
	case BooleanPointsType:
		bp, err := pr.marshalBooleans(lv)
		if err != nil {
			return err
		}
		c = &booleanSingleBatchCursor{keys: bp.Timestamps, values: bp.Values}
	case StringPointsType:
		sp, err := pr.marshalStrings(lv)
		if err != nil {
			return err
		}
		c = &stringSingleBatchCursor{keys: sp.Timestamps, values: sp.Values}
	default:
		return fmt.Errorf("unsupported points type %v", pr.pointsType)
	}

	pr.lineWriter.WriteCursor(c)
	return nil

}

func (pr *PointsReader) Values() tsm1.Values {
	return pr.values[:pr.n]
}

func (pr *PointsReader) marshalValues(pointsType MessageType, lv []byte) error {
	switch pointsType {
	case FloatPointsType:
		fp, err := pr.marshalFloats(lv)
		if err != nil {
			return err
		}
		pr.recordFloats(fp)
	case IntegerPointsType:
		ip, err := pr.marshalIntegers(lv)
		if err != nil {
			return err
		}
		pr.recordIntegers(ip)
	case UnsignedPointsType:
		up, err := pr.marshalUnsigned(lv)
		if err != nil {
			return err
		}
		pr.recordUnsigned(up)
	case BooleanPointsType:
		bp, err := pr.marshalBooleans(lv)
		if err != nil {
			return err
		}
		pr.recordBooleans(bp)
	case StringPointsType:
		sp, err := pr.marshalStrings(lv)
		if err != nil {
			return err
		}
		pr.recordStrings(sp)
	default:
		return fmt.Errorf("unsupported points type %v", pr.pointsType)
	}

	return nil
}

func (pr *PointsReader) marshalFloats(lv []byte) (*FloatPoints, error) {
	fp := &FloatPoints{}
	err := fp.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	return fp, nil
}

func (pr *PointsReader) recordFloats(fp *FloatPoints) {
	for i, t := range fp.Timestamps {
		pr.values[i] = tsm1.NewFloatValue(t, fp.Values[i])
	}
	pr.n = len(fp.Timestamps)
}

func (pr *PointsReader) marshalIntegers(lv []byte) (*IntegerPoints, error) {
	ip := &IntegerPoints{}
	err := ip.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	return ip, err
}

func (pr *PointsReader) recordIntegers(ip *IntegerPoints) {
	for i, t := range ip.Timestamps {
		pr.values[i] = tsm1.NewIntegerValue(t, ip.Values[i])
	}
	pr.n = len(ip.Timestamps)
}

func (pr *PointsReader) marshalUnsigned(lv []byte) (*UnsignedPoints, error) {
	up := &UnsignedPoints{}
	err := up.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	return up, nil
}

func (pr *PointsReader) recordUnsigned(up *UnsignedPoints) {
	for i, t := range up.Timestamps {
		pr.values[i] = tsm1.NewUnsignedValue(t, up.Values[i])
	}
	pr.n = len(up.Timestamps)
}

func (pr *PointsReader) marshalBooleans(lv []byte) (*BooleanPoints, error) {
	bp := &BooleanPoints{}
	err := bp.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	return bp, nil
}

func (pr *PointsReader) recordBooleans(bp *BooleanPoints) {
	for i, t := range bp.Timestamps {
		pr.values[i] = tsm1.NewBooleanValue(t, bp.Values[i])
	}
	pr.n = len(bp.Timestamps)
}

func (pr *PointsReader) marshalStrings(lv []byte) (*StringPoints, error) {
	sp := &StringPoints{}
	err := sp.Unmarshal(lv)
	if err != nil {
		return nil, err
	}
	return sp, nil
}

func (pr *PointsReader) recordStrings(sp *StringPoints) {
	for i, t := range sp.Timestamps {
		pr.values[i] = tsm1.NewStringValue(t, sp.Values[i])
	}
	pr.n = len(sp.Timestamps)
}

type floatSingleBatchCursor struct {
	keys     []int64
	values   []float64
	consumed bool
}

func (c *floatSingleBatchCursor) Next() (keys []int64, values []float64) {
	if c.consumed {
		return nil, nil
	}
	c.consumed = true
	return c.keys, c.values
}

func (c *floatSingleBatchCursor) Close() {
	//
}

func (c *floatSingleBatchCursor) Err() error {
	return nil
}

type integerSingleBatchCursor struct {
	keys     []int64
	values   []int64
	consumed bool
}

func (c *integerSingleBatchCursor) Next() (keys []int64, values []int64) {
	if c.consumed {
		return nil, nil
	}
	c.consumed = true
	return c.keys, c.values
}

func (c *integerSingleBatchCursor) Close() {
	//
}

func (c *integerSingleBatchCursor) Err() error {
	return nil
}

type unsignedSingleBatchCursor struct {
	keys     []int64
	values   []uint64
	consumed bool
}

func (c *unsignedSingleBatchCursor) Next() (keys []int64, values []uint64) {
	if c.consumed {
		return nil, nil
	}
	c.consumed = true
	return c.keys, c.values
}

func (c *unsignedSingleBatchCursor) Close() {
	//
}

func (c *unsignedSingleBatchCursor) Err() error {
	return nil
}

type stringSingleBatchCursor struct {
	keys     []int64
	values   []string
	consumed bool
}

func (c *stringSingleBatchCursor) Next() (keys []int64, values []string) {
	if c.consumed {
		return nil, nil
	}
	c.consumed = true
	return c.keys, c.values
}

func (c *stringSingleBatchCursor) Close() {
	//
}

func (c *stringSingleBatchCursor) Err() error {
	return nil
}

type booleanSingleBatchCursor struct {
	keys     []int64
	values   []bool
	consumed bool
}

func (c *booleanSingleBatchCursor) Next() (keys []int64, values []bool) {
	if c.consumed {
		return nil, nil
	}
	c.consumed = true
	return c.keys, c.values
}

func (c *booleanSingleBatchCursor) Close() {
	//
}

func (c *booleanSingleBatchCursor) Err() error {
	return nil
}
