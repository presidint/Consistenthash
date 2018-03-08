package auth

import (
	"bytes"
	"fmt"
	"strings"
	. "utils"
)

type AuthInfo struct {
	Key     []byte //DES加密key， key长度必须大于等于24字节
	Version string
	Stamp   string
	Fid     string
	//Ext     string
}

const (
	AUTH_CHECK_VERSION_3_0 = "3.0"
)

func (this *AuthInfo) Generate() (string, error) {
	if this.Key == nil {
		return "", fmt.Errorf("Des key is nil.")
	}

	buf := new(bytes.Buffer)
	//不同版本，不同的加密规则
	switch this.Version {
	default:
		buf.WriteString(strings.ToLower(this.Version))
		buf.WriteString(this.Stamp)
		buf.WriteString(strings.ToLower(this.Fid))
		//if len(this.Ext) > 64 {
		//	buf.WriteString(strings.ToLower(this.Ext[:64]))
		//} else {
		//	buf.WriteString(strings.ToLower(this.Ext))
		//}
	}

	dst_buf, err := TripleDesEncrypt(buf.Bytes(), this.Key[0:24])
	if err != nil {
		return "", err
	}

	auth_str := VooleMd5String(string(dst_buf))
	return auth_str, nil
}
