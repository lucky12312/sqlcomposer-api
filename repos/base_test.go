package repos

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

var db *sqlx.DB

type Schema struct {
	create string
	drop   string
}

func (s Schema) Sqlite3() (string, string) {
	return strings.Replace(s.create, `now()`, `CURRENT_TIMESTAMP`, -1), s.drop
}

func MultiExec(e sqlx.Execer, query string) {
	stmts := strings.Split(query, ";\n")
	if len(strings.Trim(stmts[len(stmts)-1], " \n\t\r")) == 0 {
		stmts = stmts[:len(stmts)-1]
	}
	for _, s := range stmts {
		_, err := e.Exec(s)
		if err != nil {
			fmt.Println(err, s)
		}
	}
}

func RunWithSchema(schema Schema, t *testing.T, test func(db *sqlx.DB, t *testing.T)) {
	runner := func(db *sqlx.DB, t *testing.T, create, drop string) {
		defer func() {
			MultiExec(db, drop)
		}()

		MultiExec(db, create)
		test(db, t)
	}
	create, drop := schema.Sqlite3()

	runner(db, t, create, drop)
}

var defaultSchema = Schema{
	create: `
CREATE TABLE fty_dictionary_type (
	sid text,
	code text,
	name text,
	status integer,
	description text,
	create_time timestamp default now(),
	update_time timestamp default now(),
	is_delete integer default 0,
	parent_code text
);
CREATE TABLE fty_obj_attr (
	sid text,
	attr_sid text,
	obj_sid text,
	attr_value text
);
CREATE TABLE fty_product (
	sid text,
	name text
);
CREATE TABLE users (
	uid integer,
	name text,
	age integer
);
CREATE TABLE orders (
	id integer,
	uid integer,
	order_no text,
	total_amount float,
    product_sid text
);
`,
	drop: `
drop table fty_dictionary_type;
drop table fty_obj_attr;
drop table fty_product;
drop table users;
drop table orders;
`,
}

func init() {
	sqdsn := os.Getenv("SQLITE_DSN")

	if sqdsn == "" {
		sqdsn = ":memory:"
	}
	db = sqlx.MustConnect("sqlite3", sqdsn)
}

func loadDefaultFixture(db *sqlx.DB, t *testing.T) {
	tx := db.MustBegin()
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('87c53961debe28ecaf55dfc5af1c9039','prod-material','产品材质',1,'材质','2019-11-04 11:01:35','2019-11-04 11:07:53',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('af37d15ade63f26ee566fcd9692c63d4','prod-weight','产品单重',1,'单重（g）','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('416d89cf8c45218b634dd3be2ffef0f8','prod-hardness','产品硬度',1,'硬度','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('7fcba4153718271241b71a6bb941b454','prod-perform-level','产品性能等级',1,'性能等级','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))
	tx.MustExec(tx.Rebind("INSERT INTO fty_dictionary_type (sid,code,name,status,description,create_time,update_time,is_delete) VALUES ('ff6d4b42ce74bb424257e15f02f6ec63','prod-surface-treat','产品表面处理',1,'表面处理','2019-11-04 11:04:39','2019-12-14 00:20:31',0)"))

	tx.MustExec(tx.Rebind("INSERT INTO users (uid, name, age) VALUES (?, ?, ?)"), 1, "Scott", "20")
	tx.MustExec(tx.Rebind("INSERT INTO users (uid, name, age) VALUES (?, ?, ?)"), 2, "Barry", "24")
	tx.MustExec(tx.Rebind("INSERT INTO users (uid, name, age) VALUES (?, ?, ?)"), 3, "Zoe", "24")

	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 1, 1, "001", 101.1)
	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 2, 1, "002", 91.8)
	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 3, 2, "003", 28.8)
	tx.MustExec(tx.Rebind("INSERT INTO orders (id, uid, order_no, total_amount) VALUES (?, ?, ?, ?)"), 4, 3, "004", 18.9)
	_ = tx.Commit()
}

func TestCombine(t *testing.T) {
	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
	}
	f2 := &[]Filter{
		{Val: "barry", Op: Equal, Attr: "nickname"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	s1, _ := WhereAnd(f1)
	s2, _ := WhereAnd(f2)

	combined := CombineOr(s1, s2)
	assert.Equal(t, "(name = :name) OR (nickname = :nickname AND age = :age AND fav IN(:fav))", combined.Clause)
	assert.Equal(t, map[string]interface{}{
		"nickname": "barry",
		"name":     "wang",
		"age":      10,
		"fav":      []string{"pet", "movie"},
	}, combined.Arg)

	f3 := &[]Filter{
		{Val: []int{10, 15}, Op: Between, Attr: "age"},
		{Val: nil, Op: IsNotNull, Attr: "class"},
	}

	s3, _ := WhereAnd(f3)

	combined = CombineAnd(combined, s3)
	assert.Equal(t, "((name = :name) OR (nickname = :nickname AND age = :age AND fav IN(:fav))) AND (age > :age_1 AND age < :age_2 AND class IS NOT NULL)", combined.Clause)
	assert.Equal(t, map[string]interface{}{
		"nickname": "barry",
		"name":     "wang",
		"age":      10,
		"age_1":    int64(10),
		"age_2":    int64(15),
		"fav":      []string{"pet", "movie"},
	}, combined.Arg)

	// Empty combine
	filterEmpty := &[]Filter{}

	s4, _ := WhereOr(filterEmpty)

	emptyCombined := CombineAnd(s3, s4)
	assert.Equal(t, "(age > :age_1 AND age < :age_2 AND class IS NOT NULL)", emptyCombined.Clause)
	assert.Equal(t, map[string]interface{}{
		"age_1": int64(10),
		"age_2": int64(15),
	}, emptyCombined.Arg)
}

func TestBuildWhereAnd(t *testing.T) {
	f0 := &[]Filter{
		{Val: "", Op: Equal, Attr: "name"},
	}

	s0, _ := WhereAnd(f0)

	assert.Equal(t, "name = :name", s0.Clause)
	assert.Equal(t, map[string]interface{}{"name": ""}, s0.Arg)

	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "tb.name"},
	}

	s1, _ := WhereAnd(f1)

	assert.Equal(t, "tb.name = :tb_name", s1.Clause)
	assert.Equal(t, map[string]interface{}{"tb_name": "wang"}, s1.Arg)

	f2 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	s2, _ := WhereAnd(f2)
	assert.Equal(t, "name = :name AND age = :age AND fav IN(:fav)", s2.Clause)
	assert.Equal(t, map[string]interface{}{
		"name": "wang",
		"age":  10,
		"fav":  []string{"pet", "movie"},
	}, s2.Arg)

	f3 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: []int{10, 15}, Op: Between, Attr: "age"},
		{Val: nil, Op: IsNotNull, Attr: "class"},
	}

	s3, _ := WhereAnd(f3)
	assert.Equal(t, "name = :name AND age > :age_1 AND age < :age_2 AND class IS NOT NULL", s3.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":  "wang",
		"age_1": int64(10),
		"age_2": int64(15),
	}, s3.Arg)

	f4 := &[]Filter{
		{Val: "xian", Op: Contains, Attr: "name"},
		{Val: "wang", Op: StartsWith, Attr: "nickname"},
		{Val: "barry", Op: EndsWith, Attr: "firstName"},
	}

	s4, err := WhereAnd(f4)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "name LIKE :name AND nickname LIKE :nickname AND firstName LIKE :firstName", s4.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":      "%xian%",
		"nickname":  "wang%",
		"firstName": "%barry",
	}, s4.Arg)

	f5 := &[]Filter{
		{Val: "中文", Op: Contains, Attr: "cust_name"},
	}

	s5, err := WhereAnd(f5)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "cust_name LIKE :cust_name", s5.Clause)
	assert.Equal(t, map[string]interface{}{
		"cust_name": "%中文%",
	}, s5.Arg)
}

func TestBuildWhereOr(t *testing.T) {
	fe := &[]Filter{
		{Val: "", Op: Equal, Attr: "fty_plan_package.batch_no"},
		{Val: "", Op: IsNull, Attr: "fty_plan_package.batch_no"},
	}

	se, _ := WhereOr(fe)
	assert.Equal(t, "fty_plan_package.batch_no = :fty_plan_package_batch_no OR fty_plan_package.batch_no IS NULL", se.Clause)

	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
	}

	s1, _ := WhereOr(f1)

	assert.Equal(t, "name = :name", s1.Clause)
	assert.Equal(t, map[string]interface{}{"name": "wang"}, s1.Arg)

	f2 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	s2, _ := WhereOr(f2)
	assert.Equal(t, "name = :name OR age = :age OR fav IN(:fav)", s2.Clause)
	assert.Equal(t, map[string]interface{}{
		"name": "wang",
		"age":  10,
		"fav":  []string{"pet", "movie"},
	}, s2.Arg)

	f3 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: []int{10, 15}, Op: Between, Attr: "age"},
		{Val: nil, Op: IsNotNull, Attr: "class"},
	}

	s3, _ := WhereOr(f3)
	assert.Equal(t, "name = :name OR age > :age_1 AND age < :age_2 OR class IS NOT NULL", s3.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":  "wang",
		"age_1": int64(10),
		"age_2": int64(15),
	}, s3.Arg)

	f4 := &[]Filter{
		{Val: "xian", Op: Contains, Attr: "name"},
		{Val: "wang", Op: StartsWith, Attr: "nickname"},
		{Val: "barry", Op: EndsWith, Attr: "firstName"},
	}

	s4, err := WhereOr(f4)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "name LIKE :name OR nickname LIKE :nickname OR firstName LIKE :firstName", s4.Clause)
	assert.Equal(t, map[string]interface{}{
		"name":      "%xian%",
		"nickname":  "wang%",
		"firstName": "%barry",
	}, s4.Arg)

	f5 := &[]Filter{
		{Val: "中文", Op: Contains, Attr: "cust_name"},
	}

	s5, err := WhereOr(f5)

	if err != nil {
		t.Log(err)
	}
	assert.Equal(t, "cust_name LIKE :cust_name", s5.Clause)
	assert.Equal(t, map[string]interface{}{
		"cust_name": "%中文%",
	}, s5.Arg)
}

type FulltextSearchExpander struct {
	Fields []string
}

func (e *FulltextSearchExpander) Expand(origFilter Filter) (FilterStmt, error) {
	var filters []Filter
	filters = []Filter{}

	for _, field := range e.Fields {
		filters = append(filters, Filter{
			Attr: field,
			Op:   origFilter.Op,
			Val:  origFilter.Val,
		})
	}

	return WhereOr(&filters)
}

func TestFilterToWhereAnd(t *testing.T) {
	p1 := FilterPipeline{
		Attr:      "name",
		CombineOp: AND,
		Expander: &FulltextSearchExpander{
			Fields: []string{
				"first_name",
				"nick_name",
			},
		},
	}

	f1 := &[]Filter{
		{Val: "wang", Op: Equal, Attr: "name"},
		{Val: 10, Op: Equal, Attr: "age"},
		{Val: []string{"pet", "movie"}, Op: In, Attr: "fav"},
	}

	stmt, _ := FilterToWhereAnd(f1, p1)

	assert.Equal(t, "(first_name = :first_name OR nick_name = :nick_name) AND (age = :age AND fav IN(:fav))", stmt.Clause)
	assert.Equal(t, map[string]interface{}{
		"first_name": "wang",
		"nick_name":  "wang",
		"age":        10,
		"fav":        []string{"pet", "movie"},
	}, stmt.Arg)
}

func TestTokenReplace(t *testing.T) {
	str := "SELECT * FROM tb %foo %where %limit"

	where, _ := WhereAnd(&[]Filter{
		{Val: "中文", Op: Contains, Attr: "cust_name"},
	})

	ctx := map[string]interface{}{
		"where": where,
		"limit": SqlLimit{
			Offset: 0,
			Size:   10,
		},
		"foo": "LEFT JOIN ltb ON ltb.fid = tb.id",
	}

	r, _ := tokenReplace(str, ctx)
	assert.Equal(t, "SELECT * FROM tb LEFT JOIN ltb ON ltb.fid = tb.id WHERE cust_name LIKE :cust_name LIMIT 0, 10", r)
}

func TestNewSqlBuilder(t *testing.T) {
	var sqlComposition = `
info:
  name: example
  version: 1.0.0
composition:
  fields:
    base:
      - name: name
        expr: users.name
      - name: age
        expr: users.age
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: "SELECT %fields.base, %fields.statistic FROM users LEFT JOIN orders ON orders.uid = users.uid %where GROUP BY users.uid %limit"
  total: "SELECT count(users.uid) FROM users LEFT JOIN order ON order.uid = users.uid %where GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlComposition))

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "example", sb.Doc.Info.Name)
		assert.Equal(t, "1.0.0", sb.Doc.Info.Version)
		assert.Equal(t, "consume_times", sb.Doc.Composition.Fields["statistic"][0].Name)
		assert.Equal(t, "COUNT(orders.id)", sb.Doc.Composition.Fields["statistic"][0].Expr)

		where, err := WhereAnd(&[]Filter{
			{Val: "Barry", Op: Contains, Attr: "users.name"},
		})

		if err != nil {
			t.Fatal(err)
		}

		stmt, err := sb.AndConditions(&where).Limit(0, 10).BuildQuery()

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"WHERE (users.name LIKE ?) GROUP BY users.uid LIMIT 0, 10", stmt.QueryString)

		rows, err := stmt.Queryx(where.Arg)

		assert.Equal(t, true, rows.Next())
		row := make(map[string]interface{})
		err = rows.MapScan(row)

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "Barry", row["name"])
		assert.Equal(t, int64(24), row["age"])
		assert.Equal(t, int64(1), row["consume_times"])
		assert.Equal(t, 28.8, row["consume_total"])
	})
}

type attrsTokenReplacer struct {
	Attrs map[string]string
	DB    *sqlx.DB
}

func (atr attrsTokenReplacer) TokenReplace(ctx map[string]interface{}) string {
	return ProductAttrsToJoinInStat(atr.DB, atr.Attrs)
}

type attrsFieldsTokenReplacer struct {
	Attrs map[string]string
}

func (atr attrsFieldsTokenReplacer) TokenReplace(ctx map[string]interface{}) string {
	return ProductAttrsToSelect(atr.Attrs)
}


func TestSqlBuilder_RegisterToken(t *testing.T) {
	var sqlComposition = `
info:
  name: example
  version: 1.0.0
composition:
  tokens:
    attrs:
      params:
        - name: prod-weight
          value: product_weight
        - name: prod-material
          value: product_material
    attrs_fields:
      params:
        - name: prod-weight
          value: product_weight
        - name: prod-material
          value: product_material
  fields:
    base:
      - name: name
        expr: users.name
      - name: age
        expr: users.age
    statistic:
      - name: consume_times
        expr: COUNT(orders.id)
      - name: consume_total
        expr: SUM(orders.total_amount)
  subject: "SELECT %fields.base, %fields.statistic, %attrs_fields FROM users LEFT JOIN orders ON orders.uid = users.uid LEFT JOIN fty_product ON orders.product_sid = fty_product.sid %attrs %where GROUP BY users.uid %limit"
  total: "SELECT count(users.uid) FROM users LEFT JOIN orders ON orders.uid = users.uid LEFT JOIN fty_product ON orders.product_sid = fty_product.sid %attrs %where GROUP BY users.uid"`

	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)

		sb, err := NewSqlBuilder(db, []byte(sqlComposition))

		keys := make([]string, len(sb.Doc.Composition.Tokens))

		i := 0
		for k := range sb.Doc.Composition.Tokens {
			keys[i] = k
			i++
		}

		assert.Equal(t, []string{"attrs", "attrs_fields"}, keys)

		if err != nil {
			t.Fatal(err)
		}

		where, err := WhereAnd(&[]Filter{
			{Val: "Barry", Op: Contains, Attr: "users.name"},
		})

		if err != nil {
			t.Fatal(err)
		}

		stmt, err := sb.AndConditions(&where).Limit(0, 10).BuildQuery()

		assert.Error(t, err)
		//
		//attrs := map[string]string{
		//	"prod-weight":   "product_weight",
		//	"prod-material": "product_material",
		//}

		err = sb.RegisterToken("attrs", func(params []TokenParam) TokenReplacer {
			attrs := map[string]string{}
			for _, p := range params {
				attrs[p.Name] = p.Value
			}

			return attrsTokenReplacer{
				Attrs: attrs,
				DB:    db,
			}
		})

		if err != nil {
			t.Fatal(err)
		}

		err = sb.RegisterToken("attrs_fields", func(params []TokenParam) TokenReplacer {
			attrs := map[string]string{}
			for _, p := range params {
				attrs[p.Name] = p.Value
			}

			return attrsFieldsTokenReplacer{
				Attrs: attrs,
			}
		})

		if err != nil {
			t.Fatal(err)
		}

		stmt, err = sb.BuildQuery()

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT users.name AS name, users.age AS age, COUNT(orders.id) AS consume_times, "+
			"SUM(orders.total_amount) AS consume_total, prod_material.attr_value AS product_material,"+
			"prod_weight.attr_value AS product_weight "+
			"FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"LEFT JOIN fty_product ON orders.product_sid = fty_product.sid "+
			"LEFT JOIN fty_obj_attr AS prod_material ON prod_material.attr_sid = '87c53961debe28ecaf55dfc5af1c9039' "+
			"AND prod_material.obj_sid = fty_product.sid LEFT JOIN fty_obj_attr AS prod_weight "+
			"ON prod_weight.attr_sid = 'af37d15ade63f26ee566fcd9692c63d4' AND prod_weight.obj_sid = fty_product.sid "+
			"WHERE (users.name LIKE ?) GROUP BY users.uid LIMIT 0, 10", stmt.QueryString)

		totalStmt, err := sb.BuildTotalQuery()

		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "SELECT count(users.uid) "+
			"FROM users LEFT JOIN orders ON orders.uid = users.uid "+
			"LEFT JOIN fty_product ON orders.product_sid = fty_product.sid "+
			"LEFT JOIN fty_obj_attr AS prod_material ON prod_material.attr_sid = '87c53961debe28ecaf55dfc5af1c9039' "+
			"AND prod_material.obj_sid = fty_product.sid LEFT JOIN fty_obj_attr AS prod_weight "+
			"ON prod_weight.attr_sid = 'af37d15ade63f26ee566fcd9692c63d4' AND prod_weight.obj_sid = fty_product.sid "+
			"WHERE (users.name LIKE ?) GROUP BY users.uid", totalStmt.QueryString)
	})
}
