package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type DatabaseParam struct {
	Host     string
	Port     int
	User     string
	Password string
}

func main() {
	var (
		err              error
		GTIDMode         string
		masterServerUuid string
		masterServerId   int64

		param = DatabaseParam{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "slave_user",
			Password: "slave_password",
		}
	)

	// 检查参数
	{
		if GTIDMode, masterServerUuid, masterServerId, err = getMasterInfo(param); err != nil {
			log.Println(err)
			return
		} else {
			log.Printf("GTIDMode: %v, masterServerUuid: %v, masterServerId: %v", GTIDMode, masterServerUuid, masterServerId)
		}

		if GTIDMode != "ON" {
			return
		}

		if _, err = uuid.Parse(masterServerUuid); err != nil {
			log.Println(err)
			return
		}

		if masterServerId <= 0 {
			log.Printf("error masterServerId %v", masterServerId)
			return
		}
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", param.Host, param.Port)
	cfg.User = param.User
	cfg.Password = param.Password
	cfg.Flavor = "mysql"
	cfg.UseDecimal = true
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("create canal err %v", err)
		return
	}

	c.SetEventHandler(&handler{})

	go func() {
		var (
			set mysql.GTIDSet
			uid = fmt.Sprintf("%s:%d-%d", masterServerUuid, 1, 2)
		)

		if set, err = mysql.ParseMysqlGTIDSet(uid); err != nil {
			log.Printf("uuid: %v. %v", uid, err)
			return
		}

		if err = c.StartFromGTID(set); err != nil {
			fmt.Printf("start canal err %v", err)
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sc

	c.Close()
}

type handler struct {
	canal.DummyEventHandler
}

func (h *handler) OnRow(e *canal.RowsEvent) error {
	log.Printf("OnRow(%v.%v) %v", e.Table.Schema, e.Table.Name, e)

	var getObject = func(tab *schema.Table, row []any) map[string]any {
		if len(tab.Columns) != len(row) {
			return nil
		}

		var obj = make(map[string]any)

		for i := 0; i < len(row); i++ {
			obj[tab.Columns[i].Name] = row[i]
		}

		return obj
	}

	switch e.Action {
	case canal.InsertAction:
		// 插入语句
		for i := 0; i < len(e.Rows); i++ {
			data, _ := json.MarshalIndent(getObject(e.Table, e.Rows[i]), "", "  ")
			log.Println("添加", string(data))
		}
	case canal.DeleteAction:
		// 删除语句
		for i := 0; i < len(e.Rows); i++ {
			data, _ := json.MarshalIndent(getObject(e.Table, e.Rows[i]), "", "  ")
			log.Println("删除", string(data))
		}
	case canal.UpdateAction:
		// 修改语句
		for i := 0; i < len(e.Rows)/2; i++ {
			data1, _ := json.MarshalIndent(getObject(e.Table, e.Rows[i*2]), "", "  ")
			data2, _ := json.MarshalIndent(getObject(e.Table, e.Rows[i*2+1]), "", "  ")
			log.Printf("修改前: %v\n修改后: %v\n", string(data1), string(data2))
		}
	default:

	}

	return nil
}

func (h *handler) String() string {
	return "TestHandler"
}

func (h *handler) OnRotate(e *replication.RotateEvent) error {
	log.Println("Rotate: ", e) // bin-log 滚动 ???
	return nil
}

func (h *handler) OnTableChanged(schema string, table string) error {
	log.Println("TableChanged: ", schema, table)
	return nil
}

func (h *handler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	// DDL语句
	buf := bytes.Buffer{}
	queryEvent.Dump(&buf)

	log.Println("DDL: ", buf.String())
	return nil
}

// getMasterInfo 获取主数据库GTID同步信息
// SELECT @@gtid_mode (SHOW GLOBAL VARIABLES LIKE 'gtid_mode')
// SHOW GLOBAL VARIABLES LIKE 'server_uuid'
// SHOW GLOBAL VARIABLES LIKE 'server_id'
func getMasterInfo(cfg DatabaseParam) (string, string, int64, error) {
	var GetGlobalValue = func(conn *client.Conn, sql string, on func(r *mysql.Result) error) error {
		if r, err := conn.Execute(sql); err != nil {
			return errors.WithStack(err)
		} else {
			err = on(r)
			r.Close()
			return err
		}
	}

	var (
		err              error
		conn             *client.Conn
		GTIDMode         string
		masterServerUuid string
		masterServerId   int64
	)

	// 连接数据库
	if conn, err = client.Connect(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), cfg.User, cfg.Password, "information_schema"); err != nil {
		return "", "", 0, errors.WithStack(err)
	} else {
		defer func() {
			_ = conn.Close()
		}()
	}

	for i := 0; i < 1; i++ {
		// 获取 gtid_mode
		if err = GetGlobalValue(conn, "SELECT @@gtid_mode", func(r *mysql.Result) error {
			if value, err := r.GetString(0, 0); err != nil {
				return errors.WithStack(err)
			} else if value == "NONE" {
				return errors.New("no value")
			} else {
				// 深度复制
				var sb strings.Builder
				sb.WriteString(value)

				GTIDMode = sb.String()
			}
			return nil
		}); err != nil {
			break
		}

		// 获取 server_uuid
		if err = GetGlobalValue(conn, "SELECT @@server_uuid", func(r *mysql.Result) error {
			if value, err := r.GetString(0, 0); err != nil {
				return errors.WithStack(err)
			} else if value == "NONE" {
				return errors.New("no value")
			} else {
				// 深度复制
				var sb strings.Builder
				sb.WriteString(value)

				masterServerUuid = sb.String()
			}
			return nil
		}); err != nil {
			break
		}

		// 获取 server_id
		if err = GetGlobalValue(conn, "SELECT @@server_id", func(r *mysql.Result) error {
			if value, err := r.GetInt(0, 0); err != nil {
				return errors.WithStack(err)
			} else if value == 0 {
				return errors.New("no value")
			} else {
				masterServerId = value
			}
			return nil
		}); err != nil {
			break
		}

		if true {
			break
		}
	}

	return GTIDMode, masterServerUuid, masterServerId, nil
}
