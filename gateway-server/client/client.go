package main

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"fmt"
)

var (
	user = "test"
	password = "123456"
	host = "10.12.165.37:6060"
	//host = "127.0.0.1:5000"
	database = "test"
)

func insert1(db *sql.DB) {
	SQL := fmt.Sprintf(`INSERT INTO mytest(id, name) VALUES("hello","world")`)
	r, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := r.RowsAffected()
	if err != nil {
		fmt.Println("get affect rows failed, err[%v]", err)
		return
	}
	fmt.Println("insert success, affected rows ", count)
}

func insert2(db *sql.DB) {
	SQL := fmt.Sprintf("INSERT INTO mytest(id, name) VALUES(?,?)")
	_, err := db.Exec(SQL, 1, 2)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func insert3(db *sql.DB) {
	a := "1234234234234"
	b := "23434"
	SQL := fmt.Sprintf(`INSERT INTO mytest(id, name) VALUES(%s,%s)`, a, b)
	_, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func insert4(db *sql.DB) {
	SQL := fmt.Sprintf(`INSERT INTO mytest(id, name) VALUES("hello","world"), ("hello","world1"), ("hello","world2"), ("hello","world3456")`)
	r, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := r.RowsAffected()
	if err != nil {
		fmt.Println("get affect rows failed, err[%v]", err)
		return
	}
	fmt.Println("insert success, affected rows ", count)
}

func insert44(db *sql.DB) {
	SQL := fmt.Sprintf(`INSERT INTO mytest(user_name, pass_word, real_name) VALUES("hewei","world", "sdf"), ("yangxiao","world1", "ssdf"), ("zhangqian","world2", "kbcd"), ("chenchaofeng","world3456", "kljnvd")`)
	r, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := r.RowsAffected()
	if err != nil {
		fmt.Println("get affect rows failed, err[%v]", err)
		return
	}
	fmt.Println("insert success, affected rows ", count)
}

func insert444(db *sql.DB) {
	SQL := fmt.Sprintf(`INSERT INTO split(id, name) VALUES("hello","world"), ("hello","world1"), ("hello","world2"), ("hello","world3456")`)
	r, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := r.RowsAffected()
	if err != nil {
		fmt.Println("get affect rows failed, err[%v]", err)
		return
	}
	fmt.Println("insert success, affected rows ", count)
}

func select1(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM mytest where id="hello" AND name="world"`)
	var a, b string
	err := db.QueryRow(SQL).Scan(&a, &b)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(a, b)
}

func select2(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM mytest where id="hello"`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func select22(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM mytest where user_name="hewei"`)
	var a, b, c string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b,&c)
		fmt.Println(a, b, c)
	}
	fmt.Println("select ok")
}

func select222(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM split where id="hello"`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func select3(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM mytest`)
	var a, b string
	err := db.QueryRow(SQL).Scan(&a, &b)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(a, b)
}

func select4(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM mytest where id="hello" limit 0, 2`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func select5(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM mytest where id="hello" limit 2, 5`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func select6(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT * FROM mytest where id="hello"`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func select7(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT * FROM mytest`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func select8(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM mytest limit 5`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func select9(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT id, name FROM mytest where id = "id18"`)
	var a, b string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b)
		fmt.Println(a, b)
	}
	fmt.Println("select ok")
}

func delete(db *sql.DB) {
	SQL := fmt.Sprintf(`DELETE FROM mytest where id="hello" AND name="world"`)
	ret, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := ret.RowsAffected()
	if err != nil {
		fmt.Println("rows affected failed, err[%v]", err)
		return
	}
	fmt.Println("delete ", count)
}

func delete1(db *sql.DB) {
	SQL := fmt.Sprintf(`DELETE FROM mytest where id="hello"`)
	ret, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := ret.RowsAffected()
	if err != nil {
		fmt.Println("rows affected failed, err[%v]", err)
		return
	}
	fmt.Println("delete ", count)
}

func delete2(db *sql.DB) {
	SQL := fmt.Sprintf(`DELETE FROM mytest`)
	ret, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := ret.RowsAffected()
	if err != nil {
		fmt.Println("rows affected failed, err[%v]", err)
		return
	}
	fmt.Println("delete ", count)
}

func delete22(db *sql.DB) {
	SQL := fmt.Sprintf(`DELETE FROM time`)
	ret, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := ret.RowsAffected()
	if err != nil {
		fmt.Println("rows affected failed, err[%v]", err)
		return
	}
	fmt.Println("delete ", count)
}

func Insert() {
	host := "192.168.192.36:5000"
	database := "db"
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, host, database)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < 10; i++{
		SQL := fmt.Sprintf(`INSERT INTO mytable(name) VALUES(name_%d)`, i)
		_, err := db.Exec(SQL)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

}

func insertandselect(db *sql.DB) {
	SQL := fmt.Sprintf(`INSERT INTO time(id, name, date, time) VALUES("hello","world", "2017-06-13", "2017-06-13 16:41:34")`)
	r, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := r.RowsAffected()
	if err != nil {
		fmt.Println("get affect rows failed, err[%v]", err)
		return
	}
	fmt.Println("insert success, affected rows ", count)
	SQL = fmt.Sprintf(`SELECT id, name, date, time FROM time where id = "hello"`)
	var a, b, d, t string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&a, &b, &d, &t)
		fmt.Println(a, b, d, t)
	}
	fmt.Println("select ok ", a, b ,d, t)
}

func insertPkOrderandselect(db *sql.DB) {
	SQL := fmt.Sprintf(`INSERT INTO pkorder(user_name, pass_word, real_name) VALUES("hewei","world", "sdf")`)
	r, err := db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err := r.RowsAffected()
	if err != nil {
		fmt.Println("get affect rows failed, err[%v]", err)
		return
	}
	SQL = fmt.Sprintf(`INSERT INTO pkorder(pass_word, user_name, real_name) VALUES("world1","yangxiao", "ssdf")`)
	r, err = db.Exec(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	count, err = r.RowsAffected()
	if err != nil {
		fmt.Println("get affect rows failed, err[%v]", err)
		return
	}
	fmt.Println("insert success, affected rows ", count)
	SQL = fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM pkorder where user_name = "hewei" AND pass_word="world"`)
	var a, b, d string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}

	for rows.Next() {
		rows.Scan(&a, &b, &d)
		fmt.Println(a, b, d)
	}
	rows.Close()
	fmt.Println("select ok ", a, b ,d)

	SQL = fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM pkorder where user_name = "yangxiao" AND pass_word="world1"`)
	rows, err = db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	for rows.Next() {
		rows.Scan(&a, &b, &d)
		fmt.Println(a, b, d)
	}
	rows.Close()
	fmt.Println("select ok ", a, b ,d)
}

func pkOrderandselect(db *sql.DB) {
	SQL := fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM pkorder where user_name = "yangxiao"`)
	var a, b, d string
	rows, err := db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}

	for rows.Next() {
		rows.Scan(&a, &b, &d)
		fmt.Println(a, b, d)
	}
	rows.Close()
	fmt.Println("select ok ", a, b ,d)

	SQL = fmt.Sprintf(`SELECT user_name, pass_word, real_name FROM pkorder where pass_word="world1"`)
	rows, err = db.Query(SQL)
	if err != nil {
		fmt.Println(err)
		return
	}
	for rows.Next() {
		rows.Scan(&a, &b, &d)
		fmt.Println(a, b, d)
	}
	rows.Close()
	fmt.Println("select ok ", a, b ,d)
}


func Select() {
	host := "192.168.192.36:5000"
	database := "test"
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, host, database)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Println(err)
		return
	}
	select8(db)
	select9(db)
}

func main() {
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, host, database)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("open mysql success")
	defer db.Close()
	//delete22(db)
	//insertPkOrderandselect(db)
	pkOrderandselect(db)
	//fmt.Println("select 1")
	//select1(db)
	//fmt.Println("select 2")
	//select2(db)
	//fmt.Println("select limit")
	//select4(db)
	//fmt.Println("select limit")
	//select5(db)
	//fmt.Println("select *")
	//select6(db)
	//fmt.Println("select * all")
	//select7(db)
	//delete1(db)
	//select222(db)
	//insert4(db)
	//delete2(db)
	//select2(db)
	//delete(db)
	//Select()
	fmt.Println("finish!!!!!")
}
