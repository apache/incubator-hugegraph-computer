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

package common

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

type TNode struct {
	Left  interface{} //TNode or string
	Right interface{} //TNode or string/float/int
	Op    token.Token
}

func CalculateForInt64(x, y interface{}, op token.Token) (bool, error) {

	xInt, xok := x.(int64)
	yInt, yok := y.(int64)
	if !xok || !yok {
		return false, errors.New(fmt.Sprintf("%v %v %v eval failed", x, op, y))
	}

	// 计算逻辑
	switch op {
	case token.EQL:
		return xInt == yInt, nil
	case token.NEQ:
		return xInt != yInt, nil
	case token.GTR:
		return xInt > yInt, nil
	case token.LSS:
		return xInt < yInt, nil
	case token.GEQ:
		return xInt >= yInt, nil
	case token.LEQ:
		return xInt <= yInt, nil
	default:
		return false, errors.New(fmt.Sprintf("unsupported  operator: %s", op.String()))
	}
}

func CalculateForString(x, y interface{}, op token.Token) (bool, error) {

	xString, xok := x.(string)
	yString, yok := y.(string)
	if !xok || !yok {
		return false, errors.New(fmt.Sprintf("%v %v %v eval failed", x, op, y))
	}

	// 计算逻辑
	switch op {
	case token.EQL: // ==
		return xString == yString, nil
	case token.NEQ: // !=
		return xString != yString, nil
	default:
		return false, errors.New(fmt.Sprintf("unsupported binary operator: %s", op.String()))
	}

}

func Parse(expr string) (*TNode, error) {

	//不过滤
	if expr == "" {
		return nil, nil
	}
	parseExpr, err := parser.ParseExpr(expr)
	if err != nil {
		return nil, err
	}

	r, err := eval(parseExpr)
	if err != nil {
		return nil, err
	}

	node, _ := r.(*TNode)
	return node, nil

}

func eval(expr ast.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		return dealBasicLit(e)

	case *ast.Ident:
		return dealIdent(e), nil

	case *ast.BinaryExpr:
		node := new(TNode)
		x, err := eval(e.X)
		if err != nil {
			return nil, err
		}
		node.Left = x

		y, err := eval(e.Y)

		if err != nil {
			return nil, err
		}

		node.Right = y
		node.Op = e.Op

		return node, nil

	default:
		return nil, errors.New(fmt.Sprintf("expression has not support type"))
	}

}

func dealBasicLit(basic *ast.BasicLit) (interface{}, error) {
	switch basic.Kind {
	case token.INT:
		value, err := strconv.ParseInt(basic.Value, 10, 64)
		if err != nil {
			return nil, err
		}
		return value, nil
	case token.STRING:
		value, err := strconv.Unquote(basic.Value)
		if err != nil {
			return nil, err
		}
		return value, nil
	}
	return nil, errors.New(fmt.Sprintf("%s is not support type", basic.Kind))
}

func dealIdent(indent *ast.Ident) interface{} {
	return indent.Name
}
