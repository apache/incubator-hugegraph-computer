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

package main

import (
	"encoding/json"
	"flag"
	"math"
	"vermeer/apps/auth"
)

func main() {
	user := flag.String("user", "foo", "user name")
	space := flag.String("space", "bar", "space name")
	factor := flag.String("factor", "1234", "token factor")
	client := flag.String("client", "hg", "client identifier")
	immortal := flag.Bool("i", false, "immortal token")
	flag.Parse()

	factory := &auth.TokenFactory{Factor: *factor}
	tokenObj := factory.NewToken(*user, *space, *client)
	if *immortal {
		tokenObj.Time = math.MaxInt64
	}
	factory.Sign(tokenObj)
	jsonStr := toJson(*tokenObj)
	token := auth.ToBase58(jsonStr)

	println("    user:", *user)
	println("   space:", *space)
	println("  factor:", *factor)
	println("  client:", *client)
	println("immortal:", *immortal)
	println("    json:", jsonStr)
	println("   token:", token)
}

func toJson(d interface{}) string {
	if b, err := json.Marshal(d); err == nil {
		str := string(b)
		return str
	}
	return ""
}
