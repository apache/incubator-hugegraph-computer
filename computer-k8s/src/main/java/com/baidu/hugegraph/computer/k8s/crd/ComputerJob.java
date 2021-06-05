package com.baidu.hugegraph.computer.k8s.crd;

import java.util.Map;

import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobState;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version(ComputerJob.VERSION)
@Group(ComputerJob.GROUP)
@Kind(ComputerJob.KIND)
@Plural(ComputerJob.PLURAL)
@Singular(ComputerJob.SINGULAR)
public class ComputerJob
       extends CustomResource<Map<String, Object>, ComputerJobState>
       implements Namespaced {

    private static final long serialVersionUID = 6788317713035067450L;

    public static final String VERSION = "v1";
    public static final String GROUP = "computer.hugegraph.io";
    public static final String KIND = "HugeGraphComputerJob";
    public static final String PLURAL = "hugegraphcomputerjobs";
    public static final String SINGULAR = "hugegraphcomputerjob";
    public static final String API_VERSION = GROUP + "/" + VERSION;
}
