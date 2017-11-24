package server

import (
	"strings"
	"fmt"
	"time"
	"runtime"

	"util/log"
	"model/pkg/metapb"
)

const (
	KB   uint64 = 1024
	MB          = 1024 * KB
	GB          = 1024 * MB
	PB          = 1024 * GB
)

/*
  USE 数据库名 CREATE TABLE 表名 (列名 类型(大小) DEFAULT'默认值' CONSTRAINT 约束名 约束定义,
                                列名 类型(大小) DEFAULT'默认值' CONSTRAINT 约束名 约束定义,
                                列名 类型(大小) DEFAULT'默认值' CONSTRAINT 约束名 约束定义,
                                ... ...);
    注：(1) 绿色部份是可以省略的。
       (2) 一个列是可以有多个约束的。
    约束定义：
    （1）NULL | NOT NULL  用于定义列的空值约束。(定义列)  (下面的蓝色部份是单选其中之一)
            语法：CONSTRAINT 约束名 NULL | NOT NULL
            注意：
                a. NOT NULL 约束强制列不接受 NULL 值。
                b. NOT NULL 约束强制字段始终包含值。这意味着，如果不向字段添加值，就无法插入新纪录或者更新记录。

    （2）UNIQUE  约束唯一标识数据库表中的每条记录。(即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 UNIQUE (列名, 列名, ... ...);
            说明：用于指定基本表在某一个列或多个列的组合上取值必须唯一。定义了UNIQUE约束的那些列称为唯一键。如果为基本表的革一列或多个列的组合指定了UNIQUE约束，
                 则系统将为这些列建立唯一索引，从而保证在表中的任意两行记录在指定的列或列组合上不能取同样的值。
            注意：
                    a. UNIQUE 约束唯一标识数据库表中的每条记录。
                    b. UNIQUE 和 PRIMARY KEY 约束均为列或列集合提供了唯一性的保证。
                    c. PRIMARY KEY 拥有自动定义的 UNIQUE 约束。
                    d.请注意，每个表可以有多个 UNIQUE 约束，但是每个表只能有一个 PRIMARY KEY 约束。

    （3）PRIMARY KEY 约束唯一标识数据库表中的每条记录。(即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 PRIMARY KEY (列名, 列名, ... ...);
            说明：用于定义基本表的主键。与UNIQUE约束类似，PRIMARY KEY 约束也是通过建立唯一索引来保证基本表在主键列（某一个列或多个列的组合）上取值的唯一性。
                 然而它们之间也存在着很大差别：在一个基本表中只能定义一个 PRIMARY KEY 约束，却能定义多个UNIQUE约束。
                 如果为基本表的某一个列或多个列的组合指定了 PRIMARY KEY 约束，
                 那么其中在任何一个列都不能出现空值；而 UNIQUE 约束允许出现空值。
            注意：
                    a. 主键必须包含唯一的值。
                    b. 主键列不能包含 NULL 值。
                    c. 每个表应该都一个主键，并且每个表只能有一个主键。

     （4）FOREIGN KEY 外键 (即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 FOREIGN KEY (列名, 列名, ... ...) REFERENCES (列名, 列名, ... ...) ;
            说明：指定某一个列或多个列的组合作为外部键，并在外部键和它所引用的主键或唯一键之间建立联系。在这种联系中，包含外部键的基本表称为从表，包含外部键引用的主键或唯一键的表称为主表。
                 一旦为一列或列的组合定义了 FOREIGN KEY 约束，系统将保证从表在外部键上的取值要么是主表中某一个主键值或唯一键值，要么取空值。
            注意：
                    a.在REFERENCES 中引用的列必须和 FOREIGN KEY 的外部键列一一对应，即列数目相等并且相应列的数据类型相同。

     （5）CHECK 约束用于限制列中的值的范围。 (即可以定义列也可能定义表)
            语法：CONSTRAINT 约束名 CHECK (约束条件);
            说明：用于指定基本表中的每一条记录必须满足的条件，可以对基本表在各个列上的值做进一步的约束，如成绩列的取值既不能大于100，
                 也不能小于0。
            注意：
                    a. 如果对单个列定义 CHECK 约束，那么该列只允许特定的值。
                    b. 如果对一个表定义 CHECK 约束，那么此约束会在特定的列中对值进行限制。
*/

func SqlParse(_sql string) (t *metapb.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			fn := func() string {
				n := 10000
				var trace []byte
				for i := 0; i < 5; i++ {
					trace = make([]byte, n)
					nbytes := runtime.Stack(trace, false)
					if nbytes < len(trace) {
						return string(trace[:nbytes])
					}
					n *= 2
				}
				return string(trace)
			}
			err = fmt.Errorf("panic:%v", r)
			log.Error("panic:%v", r)
			log.Error("Stack: %s", fn())
			return
		}
	}()
	sql := strings.ToLower(_sql)
	if !strings.HasPrefix(sql, "create table ") {
		fmt.Println("invalid create table")
		return nil, ErrSQLSyntaxError
	}
	// 解析出表名
	fIndex := strings.Index(sql, "(")
	if fIndex <= 0 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	if fIndex <= 13 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	tableName := string([]byte(sql)[13:fIndex])
	tableName = strings.Replace(tableName, " ", "", -1)
	if len(tableName) == 0 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	log.Debug("table name %s", tableName)
	lIndex := strings.LastIndex(sql, ")")
	if lIndex <= 0 {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	if fIndex + 1 > lIndex {
		log.Warn("sql %s invalid", sql)
		return nil, ErrSQLSyntaxError
	}
	sql = string([]byte(sql)[fIndex + 1:lIndex])
	sql = strings.Replace(sql, "\t", "", -1)
	sql = strings.Replace(sql, "\n", "", -1)
	sql = strings.Replace(sql, "\n\t", "", -1)
	colStrs := strings.Split(sql, ",")
	cols, err := ColumnParse(colStrs)
	if err != nil {
		return nil, err
	}
	t = &metapb.Table{
		Name:       tableName,
		Columns:    cols,
		Epoch:      &metapb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
	}
	return t, nil
}

func ColumnParse(cols []string) ([]*metapb.Column, error) {
	var pks, unique []string
	var fIndex, lIndex int
	var index uint64 = 0
	var columns []*metapb.Column
	for _, col := range cols {
		if strings.HasPrefix(col, "primary key") {
			fIndex := strings.Index(col, "(")
			if fIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			lIndex := strings.LastIndex(col, ")")
			if lIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			col := string([]byte(col)[fIndex + 1: lIndex])
			col = strings.Replace(col, " ", "", -1)
			pks = strings.Split(col, ",")
			log.Debug("PKS %v", pks)
		} else if strings.HasPrefix(col, "unique") {
			fIndex = strings.Index(col, "(")
			if fIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			lIndex = strings.LastIndex(col, ")")
			if lIndex <= 0 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			col := string([]byte(col)[fIndex + 1: lIndex])
			col = strings.Replace(col, " ", "", -1)
			unique = strings.Split(col, ",")
			log.Debug("UNIQUE %v", unique)
		} else if strings.Contains(col, "constraint") {
			log.Warn("sql %s invalid", col)
			return nil, fmt.Errorf("statement constraint not support now")
		} else if strings.HasPrefix(col, "check") {
			log.Warn("sql %s invalid", col)
			return nil, fmt.Errorf("statement check not support now")
		} else if strings.HasPrefix(col, "foreing key") {
			log.Warn("sql %s invalid", col)
			return nil, fmt.Errorf("statement foreing key not support now")
		} else {
			log.Debug("col %s", col)
			ctc := strings.SplitN(col, " ", 3)
			if len(ctc) < 2 {
				log.Warn("sql %s invalid", col)
				return nil, ErrSQLSyntaxError
			}
			index++
			c := &metapb.Column{Name: ctc[0], Id: index}
			err := parseColType(ctc[1], c)
			if err != nil {
				return nil, err
			}
			if len(ctc) == 3 {
				if strings.HasPrefix(ctc[2], "default") {
					dd := strings.Split(ctc[2], " ")
					_default := strings.Replace(dd[1], " ", "", -1)
					_default = strings.Replace(_default, "'", "", -1)
					log.Debug("col %s default %s", ctc[0], _default)
					c.DefaultValue = []byte(_default)
				} else if strings.HasPrefix(ctc[2], "not null") {
					c.Nullable = false
				} else if strings.HasPrefix(ctc[2], "null") {
					c.Nullable = true
				} else {
					log.Warn("sql %s invalid", col)
					return nil, ErrSQLSyntaxError
				}
			}
			columns = append(columns, c)
		}
	}
	if len(pks) == 0 {
		return nil, ErrPkMustNotNull
	}
	for _, pk := range pks {
		find := false
		for _, c := range columns {
			if c.GetName() == pk {
				if len(c.DefaultValue) != 0 {
					return nil, ErrPkMustNotSetDefaultValue
				}
				c.PrimaryKey = 1
				find = true
				break
			}
		}
		if !find {
			return nil, ErrSQLSyntaxError
		}
	}
	return columns, nil
}

/*
integer(size)
int(size)
smallint(size)
tinyint(size)
仅容纳整数。在括号内规定数字的最大位数。

float
double
decimal(size,d)
numeric(size,d)
容纳带有小数的数字。
"size" 规定数字的最大位数。"d" 规定小数点右侧的最大位数。

char(size)
容纳固定长度的字符串（可容纳字母、数字以及特殊字符）。
在括号中规定字符串的长度。

varchar(size)
容纳可变长度的字符串（可容纳字母、数字以及特殊的字符）。
在括号中规定字符串的最大长度。

date(yyyymmdd)	容纳日期。
datetime
timestamp
*/
func parseColType(_type string, col *metapb.Column) error {
	if strings.HasPrefix(_type, "integer") {
		col.DataType = metapb.DataType_Int
	} else if strings.HasPrefix(_type, "int") {
		col.DataType = metapb.DataType_Int
	} else if strings.HasPrefix(_type, "smallint") {
		col.DataType = metapb.DataType_Smallint
	} else if strings.HasPrefix(_type, "tinyint") {
		col.DataType = metapb.DataType_Tinyint
	} else if strings.HasPrefix(_type, "bigint") {
		col.DataType = metapb.DataType_BigInt
	} else if strings.HasPrefix(_type, "float") {
		col.DataType = metapb.DataType_Float
	} else if strings.HasPrefix(_type, "double") {
		col.DataType = metapb.DataType_Double
	} else if strings.HasPrefix(_type, "char") {
		col.DataType = metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "varchar") {
		col.DataType = metapb.DataType_Varchar
	} else if strings.HasPrefix(_type, "date") {
		col.DataType = metapb.DataType_Date
	} else if strings.HasPrefix(_type, "timestamp") {
		col.DataType = metapb.DataType_TimeStamp
	} else {
        return fmt.Errorf("%s not support now", _type)
	}
	return nil
}