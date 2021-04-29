package com.baidu.hugegraph.computer.core.store.select;

import java.io.IOException;
import java.util.List;

public interface InputFilesSelector {

    /**
     * Select the input files to the output files.
     * @return key is output file. value is input files.
     */
    List<SelectedFiles> selectedOfOutputs(List<String> inputs,
                                          List<String> outputs)
                                          throws IOException;
}
