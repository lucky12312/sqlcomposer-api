package repos

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProductAttrsToJoinInStat(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)
		res1 := ProductAttrsToJoinInStat(db, map[string]string{
			"prod-weight": "product_weight",
		})

		assert.Equal(t, `LEFT JOIN fty_obj_attr AS prod_weight ON prod_weight.attr_sid = 'af37d15ade63f26ee566fcd9692c63d4' AND prod_weight.obj_sid = fty_product.sid`, res1)

		res2 := ProductAttrsToJoinInStat(db, map[string]string{
			"prod-weight":   "product_weight",
			"prod-material": "product_material",
		})

		assert.Equal(t, `LEFT JOIN fty_obj_attr AS prod_material ON prod_material.attr_sid = '87c53961debe28ecaf55dfc5af1c9039' AND prod_material.obj_sid = fty_product.sid LEFT JOIN fty_obj_attr AS prod_weight ON prod_weight.attr_sid = 'af37d15ade63f26ee566fcd9692c63d4' AND prod_weight.obj_sid = fty_product.sid`, res2)
	})
}

func TestSqlMapToString(t *testing.T) {
	res1 := SqlMapToString(map[string]string{
		"plan_no":  "fty_plan_package.plan_no",
		"order_no": "fty_plan_package.order_no",
	})

	assert.Equal(t, `fty_plan_package.order_no AS order_no,fty_plan_package.plan_no AS plan_no`, res1)
}

func TestProductAttrsToSelect(t *testing.T) {
	res1 := ProductAttrsToSelect(map[string]string{
		"prod-material": "product_material",
		"prod-weight":   "product_unit_weight",
	})

	assert.Equal(t, `prod_material.attr_value AS product_material,prod_weight.attr_value AS product_unit_weight`, res1)
}

func TestGetMESDictTypes(t *testing.T) {
	RunWithSchema(defaultSchema, t, func(db *sqlx.DB, t *testing.T) {
		loadDefaultFixture(db, t)
		res := GetMESDictTypes(db)

		sid1 := res["prod-weight"]
		assert.Equal(t, "af37d15ade63f26ee566fcd9692c63d4", sid1)

		sid2 := res["prod-material"]
		assert.Equal(t, "87c53961debe28ecaf55dfc5af1c9039", sid2)
	})
}