package microservice

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//Data Struct
type Data struct {
	Payload             interface{} `json:"payload"`
	TimeP, TimeD, TimeS int

	TSsIN,
	TSrqIN,
	TSrqOUT,
	TSpIN,
	TSpOUT,
	TSdqIN,
	TSdqOUT,
	TSdIN,
	TSdOUT,
	TSsOUT int64
}

func (d Data) String() string {
	return "Payload: " + d.Payload.(string) +
		" TimeP: " + strconv.Itoa(d.TimeP) +
		" TimeD: " + strconv.Itoa(d.TimeD) +
		" TimeS: " + strconv.Itoa(d.TimeS)
}

//CFG Struct
type CFG struct {
	Name                         string
	MaxRQSize, MaxDQSize         int
	RQBusy, DQBusy, SQBusy       bool
	MaxPWorkerNum, MaxDWorkerNum int
	FailRetry                    int
	MonitorWindow                int
	WorkerTimeOut                int
}

//MN Struct
type MN struct {
	Name                                     string
	ProcessCount, DeliveryCount              int
	ActivePWorkers, ActiveDWorkers           int
	RQLength, DQLength, MQLength, MSGQLength int
	PFailCount, DFailCount                   int
	RejectCount                              int

	msProcessCount, msDeliveryCount, msProcessWorkerCount, msDeliveryWorkerCount prometheus.Counter
	msActProcessWorkerNum, msActDeliveryWorkerNum                                prometheus.Gauge
	msRQLengthCount, msDQLengthCount, msMQLengthCount, msMSGQLengthCount         prometheus.Counter
	msRQArriveCount, msDQArriveCount, msRQBlockCount, msDQBlockCount             prometheus.Counter
	msServiceTimeHistogram                                                       prometheus.Histogram
}

//MS Struct
type MS struct {
	Name, Host string
	Port       int
	RQ, DQ, MQ chan *Data
	MSGQ       chan Message
	CFG
	MN

	//mutex sync.Mutex
}

//Message type
type Message int

//_ message
const (
	_ Message = iota
	RQBLOCKED
	DQBLOCKED
	MQBLOCKED

	RQARRIVED
	DQARRIVED

	RQPROCESSED
	DQDELIVERED

	COMPLETED

	PWORKERSTOPED
	DWORKERSTOPED

	PWORKERSTARTED
	DWORKERSTARTED

	REJECTED

	PFAILED
	DFAILED
)

//workertype
type workertype int

//P D
const (
	P workertype = iota
	D
)

func (w workertype) String() string {
	return [...]string{"Process", "Delivery"}[w]
}

//MicroService Interface
type MicroService interface {
	Receiver()
	Process(*Data) (*Data, bool)  //if data is not nil ,then it will send to dq, return false will retry if retry > 0
	Delivery(*Data) (*Data, bool) //if data is not nil ,then it will send to dq, return false will retry if retry > 0

	RestDo(http.ResponseWriter, *http.Request)
	RestConfig(http.ResponseWriter, *http.Request)
	RestMonitor(http.ResponseWriter, *http.Request)

	Run()
}

//Receiver Func
func (ms *MS) Receiver() {

}

//Process Func
func (ms *MS) Process(data *Data) (*Data, bool) {
	defer func() {
		//log.Println("Process done... ", *data)
	}()
	//log.Println("Processing... ", *data)

	time.Sleep(1 * time.Second)
	return data, true
}

//Delivery Func
func (ms *MS) Delivery(data *Data) (*Data, bool) {
	defer func() {
		//log.Println("Delivery done... ", *data)
	}()
	//log.Println("Delivering... ", *data)

	time.Sleep(1 * time.Second)
	return data, true
}

//Run Func
func (ms *MS) Run() {
	ms.Init(ms)
	log.Println("Running...", ms)
	log.Fatal(http.ListenAndServe(ms.Host+":"+strconv.Itoa(ms.Port), nil))
}

//Init Func
func (ms *MS) Init(svr MicroService) {
	ms.RQ = make(chan *Data, ms.MaxRQSize)
	ms.DQ = make(chan *Data, ms.MaxDQSize)
	//ms.RQ = make(chan *Data)
	//ms.DQ = make(chan *Data)
	ms.MQ = make(chan *Data, ms.MonitorWindow)

	ms.MSGQ = make(chan Message, 10)

	http.HandleFunc("/do", svr.RestDo)
	http.HandleFunc("/config", svr.RestConfig)
	http.HandleFunc("/monitor", svr.RestMonitor)

	ms.msProcessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_processed_total",
		Help: "The total number of processed request",
	})
	ms.msDeliveryCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_delivered_total",
		Help: "The total number of delivered request",
	})
	ms.msProcessWorkerCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_process_worker_total",
		Help: "The total number of prcoess worker",
	})
	ms.msDeliveryWorkerCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_delivery_worker_total",
		Help: "The total number of delivery worker",
	})
	ms.msActProcessWorkerNum = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ms_act_process_worker_num",
		Help: "The number of active process worker",
	})
	ms.msActDeliveryWorkerNum = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ms_act_delivery_worker_num",
		Help: "The number of active delivery worker",
	})
	ms.msRQArriveCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_rq_arrive_total",
		Help: "The total number of rqueue arrives",
	})
	ms.msRQBlockCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_rq_block_total",
		Help: "The total number of rqueue blocked",
	})
	ms.msDQArriveCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_dq_arrive_total",
		Help: "The total number of dqueue arrives",
	})
	ms.msDQBlockCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ms_dq_block_total",
		Help: "The total number of dqueue blocked",
	})
	ms.msServiceTimeHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ms_service_time_histogram",
		Help:    "A histogram of request service time.",
		Buckets: prometheus.LinearBuckets(0, 0.5, 15),
	})

	http.Handle("/metrics", promhttp.Handler())

	// go ms.dispatcher(P, ms.RQ, ms.DQ,
	// 	&ms.ProcessCount, &ms.RQBusy, &ms.DQBusy,
	// 	&ms.MaxPWorkerNum, &ms.ActivePWorkers,
	// 	ms.Process)
	// go ms.dispatcher(D, ms.DQ, ms.MQ,
	// 	&ms.DeliveryCount, &ms.DQBusy, &ms.SQBusy,
	// 	&ms.MaxDWorkerNum, &ms.ActiveDWorkers,
	// 	ms.Delivery)

	go ms.dispatcher()
	go ms.monitor()
}

//RestDo Func
func (ms *MS) RestDo(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if msg := recover(); msg != nil {
			log.Println(msg)
		}
	}()

	if r.Method != "POST" {
		msg := "Invalid request method!"
		http.Error(w, msg, http.StatusMethodNotAllowed)
		//panic(msg)
		return
	}

	if ms.RQBusy {
		msg := "System Busy!"
		http.Error(w, msg, http.StatusInternalServerError)
		ms.MSGQ <- REJECTED
		//panic(msg)
		return
	}

	var data Data
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		msg := "Error reading request body: " + err.Error()
		http.Error(w, msg, http.StatusInternalServerError)
		//panic(msg)
		return
	}

	tagdata(&data, sIN)
	ms.MSGQ <- RQARRIVED
	select {
	case ms.RQ <- &data:
		tagdata(&data, rqIN)
	default: //send message if rq blocked
		ms.MSGQ <- RQBLOCKED
		ms.RQ <- &data
		tagdata(&data, rqIN)
	}

	fmt.Fprint(w, "OK")
}

//RestConfig Func
func (ms *MS) RestConfig(w http.ResponseWriter, r *http.Request) {
	ms.CFG.Name = ms.Name
	json.NewEncoder(w).Encode(ms.CFG)
}

//RestMonitor Func
func (ms *MS) RestMonitor(w http.ResponseWriter, r *http.Request) {
	ms.RQLength = len(ms.RQ)
	ms.DQLength = len(ms.DQ)
	ms.MQLength = len(ms.MQ)
	ms.MSGQLength = len(ms.MSGQ)
	ms.MN.Name = ms.Name
	json.NewEncoder(w).Encode(ms.MN)
}

//New MicroService
func New(params map[string]interface{}) MicroService {
	//set the default params
	var (
		Name                         = "ms"
		Host                         = "localhost"
		Port                         = 36000
		MaxRQSize, MaxDQSize         = 1, 1
		MaxPWorkerNum, MaxDWorkerNum = 0, 0
		FailRetry                    = 0
		MonitorWindow                = 1
		WorkerTimeOut                = 5
	)
	//set params
	if param, ok := params["name"]; ok {
		Name = param.(string)
	}
	if param, ok := params["host"]; ok {
		Host = param.(string)
	}
	if param, ok := params["port"]; ok {
		Port = param.(int)
	}
	if param, ok := params["rqsize"]; ok {
		MaxRQSize = param.(int)
	}
	if param, ok := params["dqsize"]; ok {
		MaxDQSize = param.(int)
	}
	if param, ok := params["maxpworkernum"]; ok {
		MaxPWorkerNum = param.(int)
	}
	if param, ok := params["maxdworkernum"]; ok {
		MaxDWorkerNum = param.(int)
	}
	if param, ok := params["failretry"]; ok {
		FailRetry = param.(int)
	}
	if param, ok := params["monitorwindow"]; ok {
		MonitorWindow = param.(int)
	}
	if param, ok := params["wktimeout"]; ok {
		WorkerTimeOut = param.(int)
	}
	ms := MS{Name: Name, Host: Host, Port: Port,
		CFG: CFG{MaxRQSize: MaxRQSize, MaxDQSize: MaxDQSize,
			MaxPWorkerNum: MaxPWorkerNum, MaxDWorkerNum: MaxDWorkerNum,
			FailRetry: FailRetry, MonitorWindow: MonitorWindow,
			WorkerTimeOut: WorkerTimeOut}} //, mutex: sync.Mutex{}}

	return &ms
}

//dispatcher
func (ms *MS) dispatcher() {
	log.Println("Dispatcher Running...")
	MaxPWorkerNum := ms.MaxPWorkerNum
	MaxDWorkerNum := ms.MaxDWorkerNum
	for {
		PWorkerNum := 0
		DWorkerNum := 0
		select {
		case msg := <-ms.MSGQ:
			switch msg {
			case RQARRIVED:
				// if i := ms.workerspawner(P, ms.RQ, ms.DQ, MaxPWorkerNum, &ms.ActivePWorkers, ms.Process); i > 0 {
				// 	//log.Println("Spawned process workers", i, "Total", ms.ActivePWorkers)
				// }
				ms.msRQArriveCount.Inc()
				rl := len(ms.RQ)
				if rl == 0 { // if no data in queue
					continue
				}
				if ms.ActivePWorkers >= MaxPWorkerNum { // if no more active worker slot
					continue
				}

				PWorkerNum = MaxPWorkerNum - ms.ActivePWorkers
				if PWorkerNum >= rl {
					PWorkerNum = rl
				}

				go ms.workerspawner(P, ms.RQ, ms.DQ, PWorkerNum, ms.Process)
				ms.ActivePWorkers = ms.ActivePWorkers + PWorkerNum
				ms.msProcessWorkerCount.Add(float64(PWorkerNum))
				ms.msActProcessWorkerNum.Add(float64(PWorkerNum))

			case RQBLOCKED:
				ms.msRQBlockCount.Inc()
				//ms.RQBusy = true
				if ms.MaxPWorkerNum > 0 {
					continue
				}
				if ms.ActivePWorkers < MaxPWorkerNum {
					continue
				}

				//auto set the maxworker num
				MaxPWorkerNum++

			case DQARRIVED:
				// if i := ms.workerspawner(D, ms.DQ, ms.MQ, MaxDWorkerNum, &ms.ActiveDWorkers, ms.Delivery); i > 0 {
				// 	//log.Println("Spawned delivery workers", i, "Total", ms.ActiveDWorkers)
				// }
				ms.msDQArriveCount.Inc()
				rl := len(ms.DQ)
				if rl == 0 { // if no data in queue
					continue
				}
				if ms.ActiveDWorkers >= MaxDWorkerNum { // if no more active worker slot
					continue
				}

				DWorkerNum = MaxDWorkerNum - ms.ActiveDWorkers
				if DWorkerNum >= rl {
					DWorkerNum = rl
				}

				go ms.workerspawner(D, ms.DQ, ms.MQ, DWorkerNum, ms.Delivery)
				ms.ActiveDWorkers = ms.ActiveDWorkers + DWorkerNum
				ms.msDeliveryWorkerCount.Add(float64(DWorkerNum))
				ms.msActDeliveryWorkerNum.Add(float64(DWorkerNum))

			case DQBLOCKED:
				ms.msDQBlockCount.Inc()
				//ms.DQBusy = true0p0
				if ms.MaxDWorkerNum > 0 {
					continue
				}
				if ms.ActiveDWorkers < MaxDWorkerNum {
					continue

				}
				//auto set the maxworker num
				MaxDWorkerNum++

			case MQBLOCKED:
				//log.Println("mq blocked!")

			case RQPROCESSED:
				//ms.RQBusy = false
				ms.ProcessCount++
				ms.msProcessCount.Inc()

			case DQDELIVERED:
				//ms.DQBusy = false
				ms.DeliveryCount++
				ms.msDeliveryCount.Inc()

			case PWORKERSTOPED:
				ms.ActivePWorkers--
				ms.msActProcessWorkerNum.Dec()
				// MaxPWorkerNum = ms.ActivePWorkers
				// if MaxPWorkerNum < ms.MaxPWorkerNum {
				// 	MaxPWorkerNum = ms.MaxPWorkerNum
				// }

			case DWORKERSTOPED:
				ms.ActiveDWorkers--
				ms.msActDeliveryWorkerNum.Dec()
			// MaxDWorkerNum = ms.ActiveDWorkers
			// if MaxDWorkerNum < ms.MaxDWorkerNum {
			// 	MaxDWorkerNum = ms.MaxDWorkerNum
			// }

			case PWORKERSTARTED:
				ms.ActivePWorkers++
				ms.msActProcessWorkerNum.Inc()

			case DWORKERSTARTED:
				ms.ActiveDWorkers++
				ms.msActDeliveryWorkerNum.Inc()

			case REJECTED:
				ms.RejectCount++

			case PFAILED:
				ms.PFailCount++

			case DFAILED:
				ms.DFailCount++

			}

		}
	}
}

//workerspawner
func (ms *MS) workerspawner(wktype workertype, in, out chan *Data,
	maxworkernum int,
	f func(*Data) (*Data, bool)) int {

	//spawn new worker to match the channel length and no lager than max worker number

	i := 0
	for ; i < maxworkernum; i++ {
		id := time.Now().Nanosecond()

		//outchan := make(chan *Data)
		go func(id int, in <-chan *Data, out chan<- *Data) { //worker goroutine
			//log.Println(wktype, "Worker", id, "Started...")
			defer func() {

				switch wktype { //set timestamps
				case P:
					ms.MSGQ <- PWORKERSTOPED
				case D:
					ms.MSGQ <- DWORKERSTOPED
				}
				//log.Println(wktype, "Worker", id, "Stopped...")
			}()

			t := time.NewTimer(time.Duration(ms.WorkerTimeOut) * time.Second) //set the timer
			//t := time.NewTimer(time.Duration(rand.Intn(10)) * time.Second) //set the timer
			for {
				select {
				case data := <-in:
					if !t.Stop() { //stop the timer
						select {
						case <-t.C:
						default:
						}
					}

					switch wktype { //set timestamps
					case P:
						tagdata(data, pIN)

					case D:
						tagdata(data, dIN)

					}
					retrycount := ms.FailRetry
				exec_function:
					next, ok := safefunc(f, data) //apply process or delivery function
					switch wktype {               //set timestamps
					case P:
						tagdata(data, pOUT)
						ms.MSGQ <- RQPROCESSED
					case D:
						tagdata(data, dOUT)
						ms.MSGQ <- DQDELIVERED
					}
					if !ok {
						log.Println(wktype, "Worker", id, "processing failed...")
						switch wktype {
						case P:
							//ms.mutex.Lock()
							//ms.PFailCount++
							//ms.mutex.Unlock()
							ms.MSGQ <- PFAILED
						case D:
							//ms.mutex.Lock()
							//ms.DFailCount++
							//ms.mutex.Unlock()
							ms.MSGQ <- DFAILED
						}
						if retrycount > 0 { //retry
							retrycount--
							goto exec_function
						}
					}

					if next != nil { //send to next queue
						switch wktype {
						case P:
							ms.MSGQ <- DQARRIVED
						case D:
							ms.msServiceTimeHistogram.Observe(float64(data.TimeS) / 1000)
						}
						select {
						case out <- next:
							switch wktype {
							case P:
								tagdata(data, dqIN)
							case D:
								tagdata(data, sOUT)
							}
						default:
							switch wktype { //set timestamps
							case P:
								ms.MSGQ <- DQBLOCKED
								out <- next
								tagdata(data, dqIN)
							case D:
								ms.MSGQ <- MQBLOCKED
								//out <- next
							}
						}
					}

					t.Reset(time.Duration(ms.WorkerTimeOut) * time.Second) //reset the timer
					//t.Reset(time.Duration(rand.Intn(20)) * time.Second) //reset the timer

				case <-t.C: //time out
					return
				}
			}
		}(id, in, out)
		//workers[id] = outchan //add new worker out channel

		// switch wktype {
		// case P:
		// 	//ms.MSGQ <- PWORKERSTARTED
		// 	ms.msActProcessWorkerNum.Inc()
		// case D:
		// 	//ms.MSGQ <- DWORKERSTARTED
		// 	ms.msActDeliveryWorkerNum.Inc()
		// }
		//*activeworkernum++
	}
	return i
}

type monitordata struct {
	MonNum                       int
	Samples                      int
	MaxTimeP, AvgTimeP, MinTimeP int
	MaxTimeD, AvgTimeD, MinTimeD int
	MaxTimeS, AvgTimeS, MinTimeS int

	MinActivePWorkers, MinActiveDWorkers int
	MaxActivePWorkers, MaxActiveDWorkers int
}

func (mondata monitordata) String() string {
	return "ID: " + strconv.Itoa(mondata.MonNum) +
		" Counter: " + strconv.Itoa(mondata.Samples) +
		" MinTimeP: " + strconv.Itoa(mondata.MinTimeP) +
		" MaxTimeP: " + strconv.Itoa(mondata.MaxTimeP) +
		// " AvgTimeP: " + strconv.Itoa(mondata.AvgTimeP) +
		" MinTimeD: " + strconv.Itoa(mondata.MinTimeD) +
		" MaxTimeD: " + strconv.Itoa(mondata.MaxTimeD) +
		//" AvgTimeD: " + strconv.Itoa(mondata.AvgTimeD) +
		" MinTimeS: " + strconv.Itoa(mondata.MinTimeS) +
		" MaxTimeS: " + strconv.Itoa(mondata.MaxTimeS) +
		//" AvgTimeS: " + strconv.Itoa(mondata.AvgTimeS)
		" MinActivePWorkers: " + strconv.Itoa(mondata.MinActivePWorkers) +
		" MaxActivePWorkers: " + strconv.Itoa(mondata.MaxActivePWorkers) +
		" MinActiveDWorkers: " + strconv.Itoa(mondata.MinActiveDWorkers) +
		" MaxActiveDWorkers: " + strconv.Itoa(mondata.MaxActiveDWorkers)

}

//monitor
func (ms *MS) monitor() {
	log.Println("Monitor Running...")
	mondata := new(monitordata)
	mondata.MonNum = 0
	for {
		mondata.Samples = 0
		for ; mondata.Samples < ms.MonitorWindow; mondata.Samples++ {
			data := <-ms.MQ
			if mondata.Samples == 0 {
				mondata.MinTimeP = data.TimeP
				mondata.MinTimeD = data.TimeD
				mondata.MinTimeS = data.TimeS
				mondata.MaxTimeP = data.TimeP
				mondata.MaxTimeD = data.TimeD
				mondata.MaxTimeS = data.TimeS
				mondata.MinActiveDWorkers = ms.ActiveDWorkers
				mondata.MaxActiveDWorkers = ms.ActiveDWorkers
				mondata.MinActivePWorkers = ms.ActivePWorkers
				mondata.MaxActivePWorkers = ms.ActivePWorkers
			}
			if data.TimeD < mondata.MinTimeD {
				mondata.MinTimeD = data.TimeD
			}
			if data.TimeP < mondata.MinTimeP {
				mondata.MinTimeP = data.TimeP
			}
			if data.TimeS < mondata.MinTimeS {
				mondata.MinTimeS = data.TimeS
			}
			if data.TimeD > mondata.MaxTimeD {
				mondata.MaxTimeD = data.TimeD
			}
			if data.TimeP > mondata.MaxTimeP {
				mondata.MaxTimeP = data.TimeP
			}
			if data.TimeS > mondata.MaxTimeS {
				mondata.MaxTimeS = data.TimeS
			}
			if ms.ActiveDWorkers < mondata.MinActiveDWorkers {
				mondata.MinActiveDWorkers = ms.ActiveDWorkers
			}
			if ms.ActivePWorkers < mondata.MinActivePWorkers {
				mondata.MinActivePWorkers = ms.ActivePWorkers
			}
			if ms.ActiveDWorkers > mondata.MaxActiveDWorkers {
				mondata.MaxActiveDWorkers = ms.ActiveDWorkers
			}
			if ms.ActivePWorkers > mondata.MaxActivePWorkers {
				mondata.MaxActivePWorkers = ms.ActivePWorkers
			}
		}

		mondata.MonNum++
		log.Println("--Monitor-- ", *mondata)

	}
}

//catch panic in func
func safefunc(f func(*Data) (*Data, bool), d *Data) (*Data, bool) {
	defer func() {
		if msg := recover(); msg != nil {
			//log.Println(msg)
		}
	}()
	return f(d)
}

type ops int

const (
	_ ops = iota
	sIN
	rqIN
	rqOUT
	pIN
	pOUT
	dqIN
	dqOUT
	dIN
	dOUT
	sOUT
)

//tagdata
func tagdata(d *Data, op ops) {
	ts := time.Now().UnixNano()
	switch op {
	case sIN:
		d.TSsIN = ts
	case rqIN:
		d.TSrqIN = ts
	case rqOUT:
		d.TSrqOUT = ts
	case pIN:
		d.TSpIN = ts
	case pOUT:
		d.TSpOUT = ts
		d.TimeP = int(d.TSpOUT-d.TSpIN) / 1000 / 1000
	case dqIN:
		d.TSdqIN = ts
	case dqOUT:
		d.TSdqOUT = ts
	case dIN:
		d.TSdIN = ts
	case dOUT:
		d.TSdOUT = ts
		d.TimeD = int(d.TSdOUT-d.TSdIN) / 1000 / 1000
		d.TimeS = int(d.TSdOUT-d.TSsIN) / 1000 / 1000
	case sOUT:
		d.TSsOUT = ts
	}
}
