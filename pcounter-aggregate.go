package main

import (
	"context"
//	"encoding/json"
    "flag"
    "fmt"
    api "github.com/synerex/synerex_api"
    pbase "github.com/synerex/synerex_proto"
    pcounter "github.com/synerex/proto_pcounter"    
    "github.com/synerex/synerex_sxutil"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
//	"io/ioutil"
	"bufio"
	"github.com/yosuke-furukawa/json5/encoding/json5"
    "log"
    "os"
	"path/filepath"
	"strconv"
    "sync"
    "time"
)

//Pcounter aggregator 
//
//requires config.json to aggregate pcounter devices.
/*
  Config.json format.


*/

type aggArea struct {
	AreaName string  `json:"areaName"`
	IncDevices [][2]int `json:"incDevices",omitempty`
	DecDevices [][2]int `json:"decDevices",omitempty`
	Reset string `json:"reset",omitempty`
	AreaId uint32 `json:"id",omitempty`
	Count int32 `json:"count",omitempty`
	Timestamp *timestamp.Timestamp `json:"ts",omitempty`
	UpdateFlag bool `json:"update",omitempty`
}

type aggConfig struct {
	Devices []string  `json:"devices"`
	Areas []aggArea   `json:"areas"`
}

type aggContext struct {
	IncAreas []*aggArea
	DecAreas []*aggArea
}

// datastore provider provides Datastore Service.

type DataStore interface{
    store(str string)
}

// channel buffer for areaChan
const MAX_CHANNEL_BUFFER = 100

var (
	nodesrv    = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local		= flag.String("local","","Local Synerex Server")
	config		= flag.String("config","config.json","Config Json for aggregation")
	storeDir    = flag.String("storeDir", "aggStore", "save dir")
	interval    = flag.Int("interval", -1, "Interval of conversion. If minus, no emit, 0, realtime. Plus interavl.")
    mu         sync.Mutex
    version = "0.01"
    dataDir string
	ds DataStore
	agg aggConfig
	aggMap	*map[string]*aggContext
	areaChan  chan *aggArea
)

func init(){
    var err error
    dataDir, err =os.Getwd()
    if err != nil {
        fmt.Printf("Can't obtain current wd")
    }
    dataDir =filepath.ToSlash(dataDir) + "/" + *storeDir
    ds = &FileSystemDataStore{
        storeDir:dataDir,
    }
}

type FileSystemDataStore struct{
    storeDir string
    storeFile *os.File
    todayStr string
}

// open file with today info
func (fs *FileSystemDataStore)store(str string){
    const layout = "2006-01-02"
    day := time.Now()
    todayStr := day.Format(layout)+".csv"
    if fs.todayStr != "" && fs.todayStr != todayStr {
        fs.storeFile.Close()
        fs.storeFile = nil
    }
    if fs.storeFile == nil {
        _, er := os.Stat(fs.storeDir)
        if er != nil {// create dir
            er = os.MkdirAll(fs.storeDir, 0777)
            if er != nil {
                fmt.Printf("Can't make dir '%s'.",fs.storeDir)
                return
            }
        }
        fs.todayStr = todayStr
        file, err := os.OpenFile(filepath.FromSlash(fs.storeDir+"/"+todayStr),os.O_RDWR| os.O_CREATE | os.O_APPEND, 0666)
        if err != nil {
            fmt.Printf("Can't open file '%s'",todayStr)
            return
        }
        fs.storeFile =file
    }
    fs.storeFile.WriteString(str+"\n")
}


func supplyPCounterCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

    pc := &pcounter.PCounter{}

    err := proto.Unmarshal(sp.Cdata.Entity, pc)
	if err == nil{ // get Pcounter
//		ts0 := ptypes.TimestampString(pc.Ts)
//		ld := fmt.Sprintf("%s,%s,%s,%s,%s",ts0,pc.Hostname,pc.Mac,pc.Ip,pc.IpVpn)
//		ds.store(ld)
		for _, ev := range pc.Data {
//			line := fmt.Sprintf("%s,%s,%d,%s,%s,",ts,pc.DeviceId,ev.Seq,ev.Typ,ev.Id)
			switch (ev.Typ){
			case "counter":
				cx, ok := (*aggMap)[pc.DeviceId+"-"+ev.Id]  // should be same as deviceAggName
				if ok {
					if ev.Dir == "f" {
						for _, area := range cx.IncAreas{
							area.Count ++
							area.UpdateFlag = true
							area.Timestamp = ev.Ts
							//							log.Printf("%s %s %s :%-24s Area[%15s] ++ Count: %4d", pc.DeviceId, ev.Id, ev.Dir,ts,area.AreaName, area.Count)
							if *interval == 0{
								areaChan <- area
							}
						}
						for _, area := range cx.DecAreas{
							area.Count --
							area.UpdateFlag = true
							area.Timestamp = ev.Ts
							//							log.Printf("%s %s %s :%-24s Area[%15s] -- Count: %4d", pc.DeviceId, ev.Id, ev.Dir,ts,area.AreaName, area.Count)
							if *interval == 0{
								areaChan <- area
							}
						}
					}else if ev.Dir == "b"{
						for _, area := range cx.IncAreas{
							area.Count --
							area.UpdateFlag = true
							area.Timestamp = ev.Ts
							//							log.Printf("%s %s %s :%-24s Area[%15s] -- Count: %4d", pc.DeviceId, ev.Id, ev.Dir,ts,area.AreaName, area.Count)
							if *interval == 0{
								areaChan <- area
							}
						}
						for _, area := range cx.DecAreas{
							area.Count ++
							area.UpdateFlag = true
							area.Timestamp = ev.Ts
							//							log.Printf("%s %s %s :%-24s Area[%15s] ++ Count: %4d", pc.DeviceId, ev.Id, ev.Dir,ts,area.AreaName, area.Count)							
							if *interval == 0{
								areaChan <- area
							}
						}
					}else {
						log.Printf(":Unkown %s:  %s %s %s", ptypes.TimestampString(ev.Ts), pc.DeviceId, ev.Id, ev.Dir)
					}
				}else{
//					log.Printf("Can't find counter Device [%s] in config.json",pc.DeviceId+"-"+ev.Id )
				}
//				line = line + fmt.Sprintf("%s,%d",ev.Dir,ev.Height)
			case "fillLevel":
//				line = line + fmt.Sprintf("%d",ev.FillLevel)
			case "dwellTime":
//				tsex := ptypes.TimestampString(ev.TsExit)
//				line = line + fmt.Sprintf("%f,%f,%s,%d,%d",ev.DwellTime,ev.ExpDwellTime,tsex,ev.ObjectId,ev.Height)			
			}
//			ds.store(line)
		}
//		*/
	}
}

func subscribePCounterSupply(client *sxutil.SXServiceClient) {
    ctx := context.Background() //
    client.SubscribeSupply(ctx, supplyPCounterCallback)
    log.Fatal("Error on subscribe")
}

func deviceAggName(dev string, id int) string {
	return dev+"-"+strconv.Itoa(id)
}

func readAggConfig() *map[string]*aggContext {
	fp, err := os.Open(*config)
    if err != nil {
		fmt.Println("We need config.json file to aggregate")
        panic(err)
    }
    defer fp.Close()

    reader := bufio.NewReaderSize(fp, 64)
	
	dec := json5.NewDecoder(reader)
    
	jerr := dec.Decode(&agg)

//	fmt.Printf("%#v\n",agg)
	
	if jerr != nil {
		fmt.Println("Json format error")
		fmt.Println(jerr.Error())		
        os.Exit(1)
	}

	aggMap := make(map[string]*aggContext, len(agg.Devices))

	for i := range agg.Areas {
		v := &agg.Areas[i]
		v.UpdateFlag = true // all true for initialize
		v.AreaId = uint32(i) // set AreaId
//		fmt.Printf("Inc;%s \n", v.AreaName)
		for _, did := range v.IncDevices {
			dev := deviceAggName(agg.Devices[did[0]],did[1]) // 
			cur , ok := aggMap[dev]
			if ok {
				cur.IncAreas =append(cur.IncAreas, v)
//				fmt.Printf("Inc;%s %s %+v\n",dev, v.AreaName, *cur)
			}else {
				cur = &aggContext{
								IncAreas: make([]*aggArea,1),
								DecAreas: make([]*aggArea,0),
							}
				cur.IncAreas[0] = v
				aggMap[dev]=cur
//				fmt.Printf("NewInc;%s %s %p %+v\n",dev, v.AreaName, cur, *cur.IncAreas[0])
			}
		}
//		fmt.Printf("Dec;%s \n", v.AreaName)
		for _, did := range v.DecDevices {
			dev := deviceAggName(agg.Devices[did[0]],did[1]) // 
			cur , ok := aggMap[dev]
			if ok {
//				fmt.Printf("DecBefore %s %p %+v \n", dev, cur, *cur.IncAreas[0])
				cur.DecAreas = append(cur.DecAreas, v)
//				fmt.Printf("Dec;%s %s %p %+v %+v\n",dev, v.AreaName,cur, *cur.IncAreas[0], *cur.DecAreas[0])
			}else {
				cur = &aggContext{
								IncAreas: make([]*aggArea,0),
								DecAreas: make([]*aggArea,1),
							}
				cur.DecAreas[0] = v
				aggMap[dev]=cur
//				fmt.Printf("NewDec;%s %s %v\n",dev, v.AreaName, *cur)
			}
		}
	}

	return &aggMap
}


func supplyIntervalAcounter(client *sxutil.SXServiceClient, inter int) {
	// start supply AreaCounter
	for {
		time.Sleep(time.Duration(inter) * time.Second)

		for id := range agg.Areas {
			area := &agg.Areas[id]
			mu.Lock()
			if area.UpdateFlag {
				ts := ptypes.TimestampString(area.Timestamp)
				line := fmt.Sprintf("%s,%s,%d,%d",ts,area.AreaName, area.AreaId,area.Count)
				fmt.Println(line)
				ds.store(line)		
				ac := &pcounter.ACounter{
					Ts: area.Timestamp,
					AreaName: area.AreaName,
					AreaId : area.AreaId,

					Count : area.Count,
				}
				out, _ := proto.Marshal(ac)
				cont := api.Content{Entity: out}
				smo := sxutil.SupplyOpts{
					Name:  "ACounter",
					Cdata: &cont,
				}
				_, nerr := client.NotifySupply(&smo)
				if nerr != nil {
					log.Printf("Send Fail!\n", nerr)
		//			client.Reconnect() we need to reconsider error ...
		
				} else {
		//						log.Printf("Sent OK! %#v\n", pc)
				}
				area.UpdateFlag = false
			}
			mu.Unlock()			
		}
	}
}

func supplyChannelAcounter(client *sxutil.SXServiceClient, ch chan *aggArea) {
	for {
		area :=  <- ch  // wait for raw data
		ts := ptypes.TimestampString(area.Timestamp)
		mu.Lock()
		line := fmt.Sprintf("%s,%s,%d,%d",ts,area.AreaName, area.AreaId,area.Count)
		fmt.Println(line)
		ds.store(line)

		ac := &pcounter.ACounter{
			Ts: area.Timestamp,
			AreaName: area.AreaName,
			AreaId : area.AreaId,
			Count : area.Count,
		}

		out, _ := proto.Marshal(ac)
		cont := api.Content{Entity: out}
		smo := sxutil.SupplyOpts{
			Name:  "ACounter",
			Cdata: &cont,
		}
		_, nerr := client.NotifySupply(&smo)
		mu.Unlock()

		if nerr != nil {
			log.Printf("Send Fail!\n", nerr)
//			client.Reconnect() we need to reconsider error ...

		} else {
//						log.Printf("Sent OK! %#v\n", pc)
		}
	}
}


func main() {
	flag.Parse()
	
	//open config file.
	aggMap = readAggConfig()

	log.Printf("Config loaded")
//	log.Printf("%#v",*aggMap)
//	log.Printf("size %d",len(*aggMap))
//	for k,v := range *aggMap {
//		fmt.Printf("%s inc:%v dec:%v\n",k,(*v).IncAreas, (*v).DecAreas)
//	}

    go sxutil.HandleSigInt()    
    sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	channelTypes := []uint32{pbase.PEOPLE_COUNTER_SVC, pbase.AREA_COUNTER_SVC}
    srv, rerr := sxutil.RegisterNode(*nodesrv, "PCouterAgg", channelTypes, nil)

    if rerr != nil {
        log.Fatal("Can't register node:",rerr)
	}
	if *local != ""{// quick hack for AWS local network
		srv = *local
	}
    log.Printf("Connecting SynerexServer at [%s]", srv)

    wg := sync.WaitGroup{} // for syncing other goroutines

    client := sxutil.GrpcConnectServer(srv)

    if client == nil {
        log.Fatal("Can't connect Synerex Server")
    }else{
        log.Print("Connected with SynerexServer")
    }
    
    pc_client := sxutil.NewSXServiceClient(client, pbase.PEOPLE_COUNTER_SVC,"{Client:PcountAgg}")

    wg.Add(1)
	log.Print("Subscribe Supply PeopleCounter Channel")
	
	if *interval == 0 { // we need realtime!
		ac_client := sxutil.NewSXServiceClient(client, pbase.AREA_COUNTER_SVC,"{Client:PcountAgg}")
		areaChan = make(chan *aggArea,MAX_CHANNEL_BUFFER) // channel
		go supplyChannelAcounter(ac_client, areaChan)
	}

	go subscribePCounterSupply(pc_client)
	
	if *interval > 0 {  // if interval > 0 then make a conversion.
		ac_client := sxutil.NewSXServiceClient(client, pbase.AREA_COUNTER_SVC,"{Client:PcountAgg}")
		go supplyIntervalAcounter(ac_client, *interval) 
	}
    wg.Wait()

}


