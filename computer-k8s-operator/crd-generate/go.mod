module computer.hugegraph.io/operator

go 1.16

require (
	github.com/fabric8io/kubernetes-client/generator v0.0.0-20210604075820-b0890fa05358
	github.com/go-logr/logr v0.3.0
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
	k8s.io/api v0.20.2
	k8s.io/apimachinery v0.20.2
	k8s.io/client-go v0.20.2
	sigs.k8s.io/controller-runtime v0.8.3
)

replace k8s.io/api => k8s.io/api v0.20.2
