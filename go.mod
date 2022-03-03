module kubelet

go 1.14

require (
	github.com/docker/docker v20.10.12+incompatible
	github.com/emicklei/go-restful v2.15.0+incompatible
	github.com/fsnotify/fsnotify v1.5.1
	k8s.io/api v0.23.1
	k8s.io/apiserver v0.23.1
	k8s.io/cri-api v0.23.1
	k8s.io/klog/v2 v2.40.1
)
