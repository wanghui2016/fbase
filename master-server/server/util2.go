package server

import (
	"strings"
	"model/pkg/metapb"
	"time"
	"util/log"
	"regexp"
)

func parsePreprocessing(_sql string) (string, []string) {
	sql := strings.ToLower(_sql)
	sql = strings.TrimSpace(sql)
	var pks []string

	sql = strings.Replace(sql, "`", "", -1)
	sql = strings.Replace(sql, "\r", "", -1)
	sql = strings.Replace(sql, "\n", "", -1)
	sql = strings.Replace(sql, "\t", "", -1)

	// drop multiple space 1
	if reg, err := regexp.Compile("[ ]+"); err == nil {
		sql = reg.ReplaceAllString(sql, " ")
	}
	// drop comment
	if reg, err := regexp.Compile("comment '[^']+'"); err == nil {
		sql = reg.ReplaceAllString(sql, "")
	}
	// find pks
	if reg, err := regexp.Compile("primary key[ ]?\\(.*\\)"); err == nil {
		for _, pkeys := range reg.FindAllString(sql, -1) {
			pks_ := strings.SplitN(pkeys, "(", 2)
			if len(pks_) != 2 {
				log.Error("cannot parse prefix primay keys: no ( after 'primary key '")
				return "", nil
			}
			_pks_ := strings.SplitN(pks_[1], ")", 2)
			if len(_pks_) != 2 {
				log.Error("cannot parse prefix primay keys: no ) at end of primary key '")
				return "", nil
			}

			_pks := strings.Split(_pks_[0], ",")
			if len(_pks) == 0 {
				log.Error("not found primary key")
				return "", nil
			}
			for _, pk := range _pks {
				for _, p := range pks {
					if p == strings.TrimSpace(pk) {
						log.Error("primary key [%v] is repeated", strings.TrimSpace(pk))
						return "", nil
					}
				}
				pks = append(pks, strings.TrimSpace(pk))
			}
		}
		sql = reg.ReplaceAllString(sql, "")
	}
	// drop multiple space 2
	if reg, err := regexp.Compile("[ ]+"); err == nil {
		sql = reg.ReplaceAllString(sql, " ")
	}

	return sql, pks
}

func parseCreateTableSql(sql string) *metapb.Table {
	var primaryKeys []string
	sql, primaryKeys = parsePreprocessing(sql)

	str := strings.SplitN(sql, "(", 2)
	if len(str) != 2 {
		log.Error("cannot parse this sql: no (")
		return nil
	}
	str0 := strings.Split(str[0], " ")
	if len(str0) < 3 {
		log.Error("cannot parse tablename: len(create table <t>) < 3")
		return nil
	}

	var tableName string
	if str0[0] == "create" && str0[1] == "table" {
		tableName = strings.TrimSpace(str0[2])
	} else {
		log.Error("tablename syntax error: create table mismatch")
		return nil
	}

	var columns []*metapb.Column
	str1 := strings.Split(str[1], ",")
	for _, col_ := range str1 {
		col := strings.TrimSpace(col_)
		if len(col) == 0 {
			continue
		}
		if strings.HasSuffix(col, " primary key") {
			c := strings.Split(col, " ")
			if len(c) < 4 {
				// name type primary key
				log.Error("cannot parse primay key [%v]", c[0])
				return nil
			}
			cName := strings.TrimSpace(c[0])
			for _, p := range primaryKeys {
				if p == cName {
					log.Error("primary key [%v] is repeated", cName)
					return nil
				}
			}
			cType_ := strings.TrimSpace(c[1])
			cType := strings.Split(cType_, "(")
			cTy := dataType(cType[0])
			if cTy == metapb.DataType_Invalid {
				log.Error("invalid datetype: %v", cType_)
				return nil
			}

			columns = append(columns, &metapb.Column{
				Name: cName,
				DataType: cTy,
				PrimaryKey: 1,
			})
		} else {
			c := strings.Split(col, " ")
			cType_ := strings.TrimSpace(c[1])
			cType := strings.Split(cType_, "(")
			cTy := dataType(cType[0])
			if cTy == metapb.DataType_Invalid {
				log.Error("unsupport syntax: %v", c)
				return nil
			} else {
				cName := strings.TrimSpace(c[0])
				columns = append(columns, &metapb.Column{
					Name: cName,
					DataType: cTy,
				})
			}
		}
	}
	var id uint64
	for _, c := range columns {
		id++
		c.Id = id
		for _, p := range primaryKeys {
			if p == c.GetName() {
				c.PrimaryKey = 1
			}
		}
	}

	return &metapb.Table{
		Name:       tableName,
		Columns:    columns,
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
	}
}

func dataType(_type string) metapb.DataType {
	if strings.HasPrefix(_type, "integer") {
		return metapb.DataType_Int
	} else if strings.HasPrefix(_type, "int") {
		return metapb.DataType_Int
	} else if strings.HasPrefix(_type, "smallint") {
		return metapb.DataType_Smallint
	} else if strings.HasPrefix(_type, "tinyint") {
		return metapb.DataType_Tinyint
	} else if strings.HasPrefix(_type, "bigint") {
		return metapb.DataType_BigInt
	} else if strings.HasPrefix(_type, "float") {
		return metapb.DataType_Float
	} else if strings.HasPrefix(_type, "double") {
		return metapb.DataType_Double
	} else if strings.HasPrefix(_type, "char") {
		return metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "varchar") {
		return metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "date") {
		return metapb.DataType_Date
	} else if strings.HasPrefix(_type, "timestamp") {
		return metapb.DataType_TimeStamp
	} else {
		return metapb.DataType_Invalid
	}
}

//func test1() {
//	//var sql = `CREATE TABLE show_tag (
//	//tagid bigint(20) DEFAULT NULL COMMENT '标签Id,自增 主键',
//	//showid bigint(20) DEFAULT NULL COMMENT 'showid',
//	//createtime datetime DEFAULT NULL COMMENT '创建时间'  primary key,
//	//isvalid tinyint,
//	//PRIMARY KEY ( tagid,    showid))`
//	var sql = "CREATE TABLE `jmi_user_draw` (   `userPin` varchar(50) NOT NULL COMMENT '用户pin',   `rule` bigint(12) NOT NULL COMMENT '规则',   `drawDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '领取时间',   PRIMARY KEY (`userPin`,`rule`) )"
//
//	fmt.Println(parseCreateTableSql(sql))
//}
