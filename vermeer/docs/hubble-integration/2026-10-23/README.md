<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Hubble integration evidence — 2026-10-23

Real Chromium screenshots from a local retained development stack:

- Hubble: latest remote master `eef5afd29762f394667494b726afdce1b938214d`.
- Vermeer executable: `273e1ecf63bc739068ea08e4e2b9ff47334d94a5`.
- Synthetic graph: `DEFAULT/vermeer_review_20261023`, six vertices and seven directed edges.
- Existing anonymous development mode (`auth=none`); this run does not verify token authorization.

The Hubble helper's API smoke test completed load task `-13` and 20-step PageRank task `-14`.
Chromium submitted reload task `-15` and PageRank task `-16` with the UI defaults
(10 steps, damping 0.85). Both completed successfully; the task list and full result page rendered
all six finite ranks without page script errors. Independent PageRank iteration on the fixture
matched both outputs within `3e-8` absolute error.

1. [Loaded graph and PageRank parameters](01-vermeer-loaded-pagerank.png).
   Batch results appear in Async Tasks; the interactive canvas is unused in this flow.
2. [Successful load and compute tasks](02-vermeer-tasks-success.png).
3. [Full PageRank result](03-vermeer-pagerank-results.png).

Before validation, the retained Store exhausted its 32GB disk through 1GB Raft segments despite
its advertised 64MB option. Unused zero extents were reclaimed while Store was stopped, with all
35 segment SHA-256 checksums unchanged. A supported JRaft JVM property limited new segments to
16MB for this local environment; an actual new 16MB segment was observed. This is a local runtime
workaround. No Store binary or Toolchain source change is included here, and this run does not
claim the Store configuration propagation bug has been fixed upstream.
