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

package structure

import (
	"errors"
)

const (
	CredentialKey = "$Credential"
	AdminName     = "admin"
	AdminSpace    = "$"
	MasterName    = "master"
)

type Credential struct {
	user    string
	space   string
	isAdmin bool
}

var defaultCredential *Credential
var adminCredential *Credential

func init() {
	defaultCredential, _ = NewCredential(DefaultUserName, DefaultSpaceName)
	adminCredential, _ = NewCredential(AdminName, AdminSpace)
	adminCredential.isAdmin = true
}

func NewCredential(user string, space string) (*Credential, error) {
	if user == "" {
		return nil, errors.New("invalid user name")
	}
	if space == "" {
		return nil, errors.New("invalid space name")
	}
	return &Credential{user: user, space: space, isAdmin: user == AdminName}, nil
}

func NewAdminCred(space string) *Credential {
	return &Credential{user: AdminName, space: space, isAdmin: true}
}

func NewMasterCred(space string) *Credential {
	return &Credential{user: MasterName, space: space, isAdmin: true}
}

func (c *Credential) User() string {
	return c.user
}

func (c *Credential) Space() string {
	return c.space
}

func (c *Credential) IsAdmin() bool {
	return c.isAdmin
}

func DefaultCredential() *Credential {
	return defaultCredential
}

func AdminCredential() *Credential {
	return adminCredential
}
