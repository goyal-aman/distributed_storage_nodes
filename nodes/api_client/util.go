package apiclient

import "strings"

// MapToQueryParamStr
// return S where s= "?key1=val1&key2=val2"
// if m is empty then ""
func MapToQueryParamStr(m map[string]string) string {
	var sb strings.Builder
	len := len(m)
	count := 0
	for key, val := range m {
		if count == 0 {
			sb.WriteString("?")
		}

		sb.WriteString(key)
		sb.WriteString("=")
		sb.WriteString(val)

		if count < len-1 {
			sb.WriteString("&")
		}
		count++
	}
	return sb.String()

}
