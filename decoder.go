package csvstream

import (
	"bufio"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

const (
	TAG = "csv"
)

type fieldInfo struct {
	fldName string
	fldType reflect.Type
	tagName string
}

type Decoder struct {
	r          io.Reader
	scanner    *bufio.Scanner
	header     []string
	v          interface{}
	typ        reflect.Type
	val        reflect.Value
	fieldInfos []fieldInfo
	Delimiter  string
	HasHeader  bool
	counter    int
}

func NewDecoder(r io.Reader, v interface{}) (*Decoder, error) {

	var err error

	typ := reflect.TypeOf(v)
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Ptr is required")
	}

	typ = typ.Elem()

	if typ.Kind() != reflect.Struct {
		return nil, errors.New("Struct is required")
	}

	return &Decoder{
		r:         r,
		scanner:   bufio.NewScanner(r),
		v:         v,
		typ:       typ,
		val:       reflect.ValueOf(v).Elem(),
		HasHeader: true,
		Delimiter: ",",
		counter:   0,
	}, err

}

func (dec *Decoder) setHeader() error {
	var err error

	if dec.counter == 0 {
		if dec.HasHeader {
			_ = dec.scanner.Scan()

			dec.counter++

			if err := dec.scanner.Err(); err != nil {
				return errors.New("Failed to get Header from CSV")
			}
			dec.header = strings.Split(dec.scanner.Text(), dec.Delimiter)

		}
		dec.counter++
	}

	return err
}

func (dec *Decoder) setFieldInfos() error {
	var err error

	dec.fieldInfos = []fieldInfo{}

	for i := 0; i < dec.val.NumField(); i++ {
		fld := dec.val.Field(i)
		dec.fieldInfos = append(dec.fieldInfos,
			fieldInfo{
				fldName: dec.typ.Field(i).Name,
				fldType: fld.Type(),
				tagName: dec.typ.Field(i).Tag.Get(TAG),
			})
	}

	return err
}

func (dec *Decoder) mapHeaderToField() (map[int]fieldInfo, error) {
	var err error

	m := make(map[int]fieldInfo)

	if err := dec.setHeader(); err != nil {
		return nil, err
	}

	if err := dec.setFieldInfos(); err != nil {
		return nil, err
	}

	for i := 0; i < len(dec.header); i++ {
		for j := 0; j < len(dec.fieldInfos); j++ {
			if dec.header[i] == dec.fieldInfos[j].tagName {
				m[i] = dec.fieldInfos[j]
			}
		}
	}

	return m, err
}

func (dec *Decoder) setValue(f *reflect.Value, s string) error {

	if f.IsValid() && f.CanSet() {
		switch f.Kind() {
		case reflect.Float32, reflect.Float64:
			if floatv, err := strconv.ParseFloat(s, 64); err == nil {
				f.SetFloat(floatv)
			}
			return errors.New("Failed to convert to float")
		case reflect.String:
			f.SetString(s)
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
			if intv, err := strconv.ParseInt(s, 10, 64); err == nil {
				f.SetInt(intv)
			}
			return errors.New("Failed to convert to int")
		case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if uintv, err := strconv.ParseUint(s, 10, 64); err == nil {
				f.SetUint(uintv)
			}
			return errors.New("Failed to convert to uint")
		default:
			return errors.New("Not supported data type")
		}
	}
	return nil
}

func (dec *Decoder) Unmarshal() (<-chan interface{}, error) {
	var err error

	c := make(chan interface{})

	matched, err := dec.mapHeaderToField()
	if err != nil {
		glog.Error("Failed to map header to field")
		return nil, err
	}

	glog.Infof("%v", matched)

	// Setup new Struct based on Type
	v := reflect.New(dec.typ).Elem()

	go func() {
		defer close(c)
		for dec.scanner.Scan() {
			col := strings.Split(dec.scanner.Text(), dec.Delimiter)
			for idx, fldinfo := range matched {
				f := v.FieldByName(fldinfo.fldName)
				if err = dec.setValue(&f, col[idx]); err != nil {
					glog.Errorf("line:%d column:%s", dec.counter, fldinfo.fldName)
				}
			}
			c <- v.Interface()
		}
	}()

	return c, err
}
