/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

package auth

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
	"vermeer/apps/common"
	"vermeer/apps/structure"

	"github.com/gin-gonic/gin"
	"github.com/mr-tron/base58"

	"github.com/sirupsen/logrus"
)

const (
	HeaderKey   = "Authorization"
	CookieKey   = "vat"        // Vermeer Auth Token
	tokenMaxAge = 24 * 60 * 60 // in seconds
)

var factory *TokenFactory

func Init() {
	factor := common.GetConfig("auth_token_factor").(string)
	factory = &TokenFactory{Factor: factor}
}

func NoneAuthFilter(ctx *gin.Context) {
	credential := structure.DefaultCredential()
	logrus.Debug("default user : ", credential.User())
	logrus.Debug("default space: ", credential.Space())
	ctx.Set(structure.CredentialKey, structure.DefaultCredential())
}

func TokenFilter(ctx *gin.Context) {
	var tokenStr string
	var token *Token
	var credential *structure.Credential
	var err error
	var text string
	header := ctx.GetHeader(HeaderKey)
	if header == "" {
		if str, err := ctx.Cookie(CookieKey); err != nil {
			goto LOGIN
		} else {
			tokenStr = DecodeBase58(str)
		}
	} else {
		tokenStr = DecodeBase58(getTokenStr(header))
	}

	if tokenStr == "" {
		goto LOGIN
	}

	token = ToToken(tokenStr)
	if token == nil {
		goto LOGIN
	}

	if !factory.Verify(token) {
		text = "Invalid Token"
		goto LOGIN
	}

	if time.Now().Unix()-token.Time > tokenMaxAge {
		text = "Expired Token"
		goto LOGIN
	}

	logrus.Debugf("token: %s@%s", token.User, token.Space)

	//if structure.AdminName == token.User {
	//	credential = structure.AdminCredential()
	//} else {
	credential, err = structure.NewCredential(token.User, token.Space)
	//}
	if err != nil {
		logrus.Error(err)
		goto LOGIN
	}
	ctx.Set(structure.CredentialKey, credential)
	return

LOGIN:
	login(ctx, text)
}

func AdminFilter(ctx *gin.Context) {
	var cred *structure.Credential
	if v, ok := ctx.Get(structure.CredentialKey); ok {
		if c, ok := v.(*structure.Credential); ok {
			cred = c
		}
	}

	if cred == nil {
		login(ctx, "")
		return
	}

	if !cred.IsAdmin() {
		login(ctx, "admin permission is required")
		return
	}

}

func login(ctx *gin.Context, text string) {
	if text == "" {
		text = http.StatusText(http.StatusUnauthorized)
	}
	ctx.JSON(http.StatusUnauthorized, gin.H{
		"errcode": -1,
		"message": text,
	})
	ctx.Abort()
}

// compatible for hugegraph header: "Bearer XXXXXX"
func getTokenStr(header string) (tokenStr string) {
	if header == "" {
		return ""
	}
	if strings.HasPrefix(header, "Bearer ") {
		tokenStr = header[7:]
	} else {
		tokenStr = header
	}
	return
}

func ToTokenBase58(tokenBase58 string) *Token {
	return ToToken(DecodeBase58(tokenBase58))
}

func ToToken(tokenStr string) *Token {
	var token = new(Token)
	if err := json.Unmarshal([]byte(tokenStr), token); err == nil {
		return token
	}
	return nil
}

func ToBase58(str string) string {
	if str == "" {
		return ""
	}
	return base58.Encode([]byte(str))
}

func DecodeBase58(str string) string {
	if str == "" {
		return ""
	}

	var b []byte
	var err error
	if b, err = base58.Decode(str); err != nil {
		return ""
	}
	res := string(b)
	return res

}
