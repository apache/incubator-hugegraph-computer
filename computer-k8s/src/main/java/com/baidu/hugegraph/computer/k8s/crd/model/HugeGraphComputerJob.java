package com.baidu.hugegraph.computer.k8s.crd.model;

import java.util.Map;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

@Version(HugeGraphComputerJob.VERSION)
@Group(HugeGraphComputerJob.GROUP)
@Kind(HugeGraphComputerJob.KIND)
@Plural(HugeGraphComputerJob.PLURAL)
@Singular(HugeGraphComputerJob.SINGULAR)
public class HugeGraphComputerJob
       extends CustomResource<ComputerJobSpec, ComputerJobState>
       implements Namespaced {

    private static final long serialVersionUID = 6788317713035067450L;

    public static final String VERSION = "v1";
    public static final String GROUP = "hugegraph.baidu.com";
    public static final String KIND = "HugeGraphComputerJob";
    public static final String PLURAL = "hugegraphcomputerjobs";
    public static final String SINGULAR = "hugegraphcomputerjob";
    public static final String API_VERSION = GROUP + "/" + VERSION;

    public static ComputerJobSpec mapToSpec(Map<String, Object> specMap) {
        String yaml  = Serialization.asYaml(specMap);
        return Serialization.unmarshal(yaml, ComputerJobSpec.class);
    }
}
