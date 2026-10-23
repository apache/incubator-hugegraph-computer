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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func blockedVertexDirectory(t *testing.T) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestVertexStoreInitializationErrorAndRetry(t *testing.T) {
	vertex := &VertexInDB{}
	err := vertex.Init(blockedVertexDirectory(t))
	if err == nil || !strings.Contains(err.Error(), "initialize vertex store") {
		t.Fatalf("expected vertex storage initialization error, got %v", err)
	}
	// Failed initialization must remain safe to unload before a retry.
	vertex.freeMem()
	if err := vertex.Init(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	defer vertex.freeMem()
	vertex.workers = []*GraphWorker{{IsSelf: true}}
	vertex.AppendVertices(Vertex{ID: "retry-vertex"})
	if vertex.TotalVertexCount() != 1 || vertex.GetVertex(0).ID != "retry-vertex" {
		t.Fatal("successful retry must store the loaded vertex")
	}
}

func TestGraphPropagatesVertexStoreInitializationError(t *testing.T) {
	graph := &VermeerGraph{DataDir: blockedVertexDirectory(t)}
	graph.MallocData(GraphDataBackendOption{VertexDataBackend: DataBackendInDB})
	if err := graph.SetOption(true, true, false); err == nil {
		t.Fatal("graph initialization must report vertex storage failure")
	}
	graph.FreeMem()
}

func TestMemoryVertexInitialization(t *testing.T) {
	graph := &VermeerGraph{}
	graph.MallocData(GraphDataBackendOption{VertexDataBackend: DataBackendInMem})
	if err := graph.SetOption(true, true, false); err != nil {
		t.Fatal(err)
	}
	graph.Data.Vertex.AppendVertices(Vertex{ID: "memory-vertex"})
	if graph.Data.Vertex.GetVertex(0).ID != "memory-vertex" {
		t.Fatal("memory vertex backend must remain usable")
	}
}
