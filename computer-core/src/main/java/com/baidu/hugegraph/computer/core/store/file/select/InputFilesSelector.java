package com.baidu.hugegraph.computer.core.store.file.select;

import java.io.IOException;
import java.util.List;

public interface InputFilesSelector {

    /**
     * Select the input files to the output files.
     * @return key is output file. value is input files.
     */
    List<SelectedFiles> selectedByHgkvFile(List<String> inputs,
                                           List<String> outputs)
                                           throws IOException;

    /**
     * Select the input files to the output files.
     * @return key is output file. value is input files.
     */
    List<SelectedFiles> selectedByBufferFile(List<String> inputs,
                                             List<String> outputs)
                                             throws IOException;
}
