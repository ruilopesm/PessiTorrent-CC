package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
)

func SerializePacket(writer io.Writer, packet Packet) error {
	// First byte is the type of the struct
	err := write(writer, packet.GetPacketType())
	if err != nil {
		return err
	}

	err = SerializeStruct(writer, packet)
	if err != nil {
		return err
	}

	return nil
}

func SerializeStruct(writer io.Writer, struc interface{}) error {
	value := reflect.ValueOf(struc)

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)

		if field.CanInterface() {
			err := serializeReflectionValue(writer, field)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("can't interface with field without panicking")
		}
	}
	return nil
}

func serializeReflectionValue(writer io.Writer, field reflect.Value) error {
	var err error
	if field.Type().Kind() == reflect.Struct {
		err = SerializeStruct(writer, field.Interface())
	} else if field.Type().Kind() == reflect.Array {
		err = writeFixedSizeArray(writer, field)
	} else if field.Type().Kind() == reflect.Slice {
		err = writeArray(writer, field)
	} else {
		err = serializeField(writer, field.Interface())
	}
	if err != nil {
		return err
	}
	return nil
}

func DeserializePacket(reader io.Reader) (Packet, error) {
	// First byte is the type of the struct
	var structType uint8
	err := read(reader, &structType)
	if err != nil {
		return nil, err
	}

	packet := PacketStructFromType(structType)
	if packet == nil {
		return nil, fmt.Errorf("invalid packet type: %d", structType)
	}

	err = DeserializeToStruct(reader, packet)
	if err != nil {
		return nil, err
	}

	return packet, nil
}

func DeserializeToStruct(reader io.Reader, struc interface{}) error {
	value := reflect.ValueOf(struc)
	indirect := reflect.Indirect(value)

	for i := 0; i < indirect.NumField(); i++ {
		field := indirect.Field(i)

		err := deserializeReflectionValue(reader, field)
		if err != nil {
			return err
		}
	}

	return nil
}

func deserializeReflectionValue(reader io.Reader, field reflect.Value) error {
	var err error
	if field.Kind() == reflect.Struct {
		err = DeserializeToStruct(reader, field.Addr().Interface())
	} else if field.Kind() == reflect.Array {
		err = deserializeToFixedSizeArray(reader, field)
	} else if field.Kind() == reflect.Slice {
		err = deserializeToArray(reader, field)
	} else {
		err = deserializeToField(reader, field.Addr().Interface())
	}
	if err != nil {
		return fmt.Errorf("error deserializing field %v: %w", field.Interface(), err)
	}

	return nil
}

func deserializeToFixedSizeArray(reader io.Reader, array reflect.Value) error {
	for i := 0; i < array.Len(); i++ {
		err := deserializeReflectionValue(reader, array.Index(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func deserializeToArray(reader io.Reader, array reflect.Value) error {
	var size uint32
	err := read(reader, &size)
	if err != nil {
		return err
	}

	array.Set(reflect.MakeSlice(array.Type(), int(size), int(size)))

	for i := uint32(0); i < size; i++ {
		err := deserializeReflectionValue(reader, array.Index(int(i)))
		if err != nil {
			return err
		}
	}

	return nil
}

func serializeField(writer io.Writer, field interface{}) error {
	switch data := field.(type) {
	case uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return write(writer, data)
	case string:
		return writeString(writer, data)
	default:
		return fmt.Errorf("serialize unsupported type: %T", data)
	}
}

func deserializeToField(reader io.Reader, field any) error {
	switch data := field.(type) {
	case *uint8, *uint16, *uint32, *uint64, *int8, *int16, *int32, *int64:
		return read(reader, data)
	case *string:
		return readString(reader, data)
	default:
		return fmt.Errorf("deserialize unsupported type: %T", field)
	}
}

func writeString(writer io.Writer, data string) error {
	// Write the size of the string
	err := write(writer, uint32(len(data)))
	if err != nil {
		return err
	}

	// Write the content of the string in bytes
	err = write(writer, []uint8(data))
	if err != nil {
		return err
	}

	return nil
}

func readString(reader io.Reader, str *string) error {
	// Read the size of the string
	var size uint32
	err := read(reader, &size)
	if err != nil {
		return err
	}

	// Read the content of the string in bytes
	bytes := make([]byte, size)
	err = read(reader, &bytes)
	if err != nil {
		return err
	}

	*str = string(bytes)

	return nil
}

func writeFixedSizeArray(writer io.Writer, data reflect.Value) error {
	for i := 0; i < data.Len(); i++ {
		err := serializeReflectionValue(writer, data.Index(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func writeArray(writer io.Writer, data reflect.Value) error {
	size := data.Len()
	err := write(writer, uint32(size))
	if err != nil {
		return err
	}

	for i := 0; i < size; i++ {
		err := serializeReflectionValue(writer, data.Index(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func read(reader io.Reader, data interface{}) error {
	return binary.Read(reader, binary.LittleEndian, data)
}

func write(writer io.Writer, data interface{}) error {
	return binary.Write(writer, binary.LittleEndian, data)
}
