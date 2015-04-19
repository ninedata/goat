package goat

import (
	"encoding/json"
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"path"
)


// PrintVarInJson prints given interface using json format.
func PrintVarInJson(v interface{}) {
	b, _ := json.Marshal(v)
	var out bytes.Buffer
	json.Indent(&out, b, "", "\t")

	fmt.Printf("%s\n", out.String())
}

// PrintType print type of given interface.
func PrintType(v interface{}) {
	fmt.Printf("%s\n", reflect.TypeOf(v))
}

// GetCurrentDir returns the current directory as calling method's file
func GetCurrentDir() string {
	_, myfilename, _, _ := runtime.Caller(1)
	return path.Dir(myfilename)
}