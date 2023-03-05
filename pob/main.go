package main

import (
	"flag"
	"fmt"
	"github.com/afex/hystrix-go/hystrix"
	consulv3 "github.com/asim/go-micro/plugins/registry/consul/v3"
	ratelimit "github.com/asim/go-micro/plugins/wrapper/ratelimiter/uber/v3"
	opentracing2 "github.com/asim/go-micro/plugins/wrapper/trace/opentracing/v3"
	"github.com/asim/go-micro/v3"
	"github.com/asim/go-micro/v3/registry"
	"github.com/asim/go-micro/v3/server"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/opentracing/opentracing-go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"net"
	"net/http"
	"path/filepath"
	"pob/common"
	"pob/domain/repository"
	service2 "pob/domain/service"
	"pob/handler"
	hystrix2 "pob/plugin/hystrix"
	"pob/proto/pod"
	"strconv"
)

var (
	//注册中心配置
	consulHost       = "192.168.1.128"
	consulPort int64 = 8500
	//链路追踪
	tracerHost     = "192.168.1.128"
	tracerPort     = 6831
	hystrixPort    = 9092
	prometheusPort = 9192
)

func main() {
	//注册中心
	consul := consulv3.NewRegistry(func(options *registry.Options) {
		options.Addrs = []string{
			consulHost + ":" + strconv.FormatInt(consulPort, 10),
		}
	})
	//配置中心
	consulConfig, err := common.GetConsulConfig(consulHost, consulPort, "micro/config")
	if err != nil {
		common.Error(err)
	}

	//使用配置连接mysql
	mysqlInfo := common.GetMysqlFromConsul(consulConfig, "mysql")
	//初始化数据库
	db, err := gorm.Open("mysql", mysqlInfo.User+":"+mysqlInfo.Pwd+"@("+mysqlInfo.Host+":3306)/"+mysqlInfo.Database+"?charset=utf8&parseTime=True&loc=Local")

	if err != nil {
		common.Error(err)
	}
	defer db.Close()
	db.SingularTable(true)

	//链路追踪
	t, io, err := common.NewTracer("base", tracerHost+":"+strconv.Itoa(tracerPort))
	if err != nil {
		common.Error(err)
	}
	defer io.Close()
	opentracing.SetGlobalTracer(t)

	//有客户端或其他服务端要有熔断器，小心环状调用
	hystrixStreamHandler := hystrix.NewStreamHandler()
	hystrixStreamHandler.Start()

	go func() {
		err = http.ListenAndServe(net.JoinHostPort("0.0.0.0", strconv.Itoa(hystrixPort)), hystrixStreamHandler)
		if err != nil {
			common.Error(err)
		}
	}()

	//日志中心
	//启动filebeat
	fmt.Println("日志")

	//监控
	common.PrometheusBoot(prometheusPort)

	//下载k8s
	//连接k8s，在集群外部使用
	// -v /USER/wwc/.kube/config : /root/.kube/config
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "kubeconfig file 在当前系统的地址")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "kubeconfig file 在当前系统的地址")
	}
	flag.Parse()
	//创建config实例
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		common.Error(err)
	}
	//在集群使用
	//config,err:=rest.InClusterConfig()
	//if err!=nil{
	//	panic(err.Error())
	//}

	//创建程序客户端
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		common.Fatal(err.Error())
	}

	//启动服务实例
	service := micro.NewService(
		//自定义服务地址
		micro.Server(server.NewServer(func(options *server.Options) {
			options.Advertise = "192.168.1.128:8081"
		})),
		micro.Name("go.micro.service.pod"),
		micro.Version("latest"),
		micro.Address(":8081"),
		micro.Registry(consul),
		micro.WrapHandler(opentracing2.NewHandlerWrapper(opentracing.GlobalTracer())),
		micro.WrapClient(opentracing2.NewClientWrapper(opentracing.GlobalTracer())),
	)
	//作为客户端，添加熔断
	micro.WrapClient(hystrix2.NewClientHystrixWrapper())
	//限流
	micro.WrapHandler(ratelimit.NewHandlerWrapper(1000))

	service.Init()
	//初始化表 1次
	//err = repository.NewPodRepository(db).InitTable()
	//if err != nil {
	//	common.Fatal(err)
	//}

	//注册句柄
	podDateService := service2.NewPodDataService(repository.NewPodRepository(db), clientset)
	pod.RegisterPodHandler(service.Server(), &handler.PodHandler{PodDataService: podDateService})

	//启动服务
	err = service.Run()
	if err != nil {
		common.Fatal(err)
	}

}
