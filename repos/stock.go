package repos

import (
	"database/sql"
	"fmt"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"os"
	"sort"
	"strings"
	"sync"
)

type DictType struct {
	SID         string         `db:"sid"`
	Code        string         `db:"code"`
	Name        string         `db:"name"`
	Status      int            `db:"status"`
	Description string         `db:"description"`
	CreateTime  sql.NullString `db:"create_time"`
	UpdateTime  sql.NullString `db:"update_time"`
	IsDelete    int            `db:"is_delete"`
	ParentCode  sql.NullString `db:"parent_code"`
}

type QueryResultStockAllProduct struct {
	PlanNo              sql.NullString  `db:"plan_no"`
	OrderNo             sql.NullString  `db:"order_no"`
	ProdName            sql.NullString  `db:"prod_name"`
	ProdSid             sql.NullString  `db:"prod_sid"`
	CustMaterialNo      sql.NullString  `db:"cust_material_no"`
	CustomerSid         sql.NullString  `db:"customer_sid"`
	CustShortName       sql.NullString  `db:"cust_short_name"`
	ProductSpec         sql.NullString  `db:"product_spec"`
	ProductUnitWeight   sql.NullFloat64 `db:"product_unit_weight"`
	ProductMaterial     sql.NullString  `db:"product_material"`
	ProductHardness     sql.NullString  `db:"product_hardness"`
	ProductPerformLevel sql.NullString  `db:"product_perform_level"`
	ProductSurfaceTreat sql.NullString  `db:"product_surface_treat"`
	StockPackageAmount  sql.NullInt64   `db:"stock_package_amount"`
	StockUnitAmount     sql.NullInt64   `db:"stock_unit_amount"`
}

type QueryResultStockOutInRecords struct {
	PlanNo              sql.NullString  `db:"plan_no"`
	OrderNo             sql.NullString  `db:"order_no"`
	ProdName            sql.NullString  `db:"prod_name"`
	ProdSid             sql.NullString  `db:"prod_sid"`
	CustMaterialNo      sql.NullString  `db:"cust_material_no"`
	CustomerSid         sql.NullString  `db:"customer_sid"`
	CustShortName       sql.NullString  `db:"cust_short_name"`
	ProductSpec         sql.NullString  `db:"product_spec"`
	ProductUnitWeight   sql.NullFloat64 `db:"product_unit_weight"`
	ProductMaterial     sql.NullString  `db:"product_material"`
	ProductHardness     sql.NullString  `db:"product_hardness"`
	ProductPerformLevel sql.NullString  `db:"product_perform_level"`
	ProductSurfaceTreat sql.NullString  `db:"product_surface_treat"`
	StockPackageAmount  sql.NullInt64   `db:"stock_package_amount"`
	StockUnitAmount     sql.NullInt64   `db:"stock_unit_amount"`
	Position            sql.NullString  `db:"position"`
	WarehouseSid        sql.NullString  `db:"warehouse_sid"`
	DeliveryNo          sql.NullString  `db:"delivery_no"`
	PlanPackageNo       sql.NullString  `db:"plan_package_no"`
	StockType           sql.NullString  `db:"stock_type"`
	OperationTime       sql.NullString  `db:"operation_time"`
	Operator            sql.NullString  `db:"operator"`
	ProductAmount       sql.NullInt64   `db:"product_amount"`
	PlanPackageSid      sql.NullString  `db:"plan_package_sid"`
	WarehouseName       sql.NullString  `db:"warehouse_name"`
	WarehouseRootSid    sql.NullString  `db:"warehouse_root_sid"`
}

type QueryResultStockDistribution struct {
	PlanNo              sql.NullString  `db:"plan_no"`
	OrderNo             sql.NullString  `db:"order_no"`
	ProdName            sql.NullString  `db:"prod_name"`
	ProdSid             sql.NullString  `db:"prod_sid"`
	CustMaterialNo      sql.NullString  `db:"cust_material_no"`
	CustomerSid         sql.NullString  `db:"customer_sid"`
	CustShortName       sql.NullString  `db:"cust_short_name"`
	ProductSpec         sql.NullString  `db:"product_spec"`
	ProductUnitWeight   sql.NullFloat64 `db:"product_unit_weight"`
	ProductMaterial     sql.NullString  `db:"product_material"`
	ProductHardness     sql.NullString  `db:"product_hardness"`
	ProductPerformLevel sql.NullString  `db:"product_perform_level"`
	ProductSurfaceTreat sql.NullString  `db:"product_surface_treat"`
	StockPackageAmount  sql.NullInt64   `db:"stock_package_amount"`
	StockUnitAmount     sql.NullInt64   `db:"stock_unit_amount"`
	Position            sql.NullString  `db:"position"`
	WarehouseSid        sql.NullString  `db:"warehouse_sid"`
	WarehouseName       sql.NullString  `db:"warehouse_name"`
	WarehouseRootSid    sql.NullString  `db:"warehouse_root_sid"`
}

type QueryResultStockOrderItemUnshipped struct {
	PlanNo              sql.NullString `db:"plan_no"`
	OrderNo             sql.NullString `db:"order_no"`
	ProdName            sql.NullString `db:"prod_name"`
	ProdSid             sql.NullString `db:"prod_sid"`
	CustMaterialNo      sql.NullString `db:"cust_material_no"`
	CustomerSid         sql.NullString `db:"customer_sid"`
	CustShortName       sql.NullString `db:"cust_short_name"`
	ProductSpec         sql.NullString `db:"product_spec"`
	ProductSurfaceTreat sql.NullString `db:"product_surface_treat"`
	RemainingDays       sql.NullInt64  `db:"remaining_days"`
	RemainingStock      sql.NullInt64  `db:"remaining_stock"`
	WarehouseStockTotal sql.NullInt64  `db:"warehouse_stock_total"`
	AvailableStock      sql.NullInt64  `db:"available_stock"`
	DeliveryDate        sql.NullString `db:"delivery_date"`
	DeliveryGapStock    sql.NullInt64  `db:"delivery_gap_stock"`
	ItemOrderAmount     sql.NullInt64  `db:"item_order_amount"`
	ItemSid             sql.NullString `db:"item_sid"`
	OrderGapStock       sql.NullInt64  `db:"order_gap_stock"`
	PackageAreaStock    sql.NullInt64  `db:"package_area_stock"`
	PickAreaStock       sql.NullInt64  `db:"pick_area_stock"`
}

var once sync.Once

type DictTypesSingleton map[string]string

var dictTypes DictTypesSingleton

func GetMESDictTypes(db *sqlx.DB) DictTypesSingleton {
	once.Do(func() {
		dictTypes = DictTypesSingleton{}

		var types []DictType
		types = []DictType{}
		err := db.Select(&types, "SELECT * FROM fty_dictionary_type")

		if err != nil {
			log.Error(err)
		}

		for _, value := range types {
			dictTypes[value.Code] = value.SID
		}
	})

	return dictTypes
}

func SqlMapToString(m map[string]string) string {
	var str []string
	for key, value := range m {
		str = append(str, fmt.Sprintf("%s AS %s", value, key))
	}
	sort.Strings(str)
	return strings.Join(str, ",")
}

func ProductAttrsToSelect(a map[string]string) string {
	var str []string
	for key, value := range a {
		alias := strings.Replace(key, "-", "_", -1)
		str = append(str, fmt.Sprintf("%s.attr_value AS %s", alias, value))
	}
	sort.Strings(str)
	return strings.Join(str, ",")
}

func ProductAttrsToJoinInStat(db *sqlx.DB, a map[string]string) string {
	var str []string

	dt := GetMESDictTypes(db)

	for key, _ := range a {
		alias := strings.Replace(key, "-", "_", -1)

		sid, ok := dt[key]

		if !ok {
			os.Exit(500)
		}

		str = append(str,
			fmt.Sprintf(`LEFT JOIN fty_obj_attr AS %s ON %s.attr_sid = '%s' AND %s.obj_sid = fty_product.sid`,
				alias, alias, sid, alias))
	}
	sort.Strings(str)
	return strings.Join(str, " ")
}

func GetFinishedProductOutInRecords(db *sqlx.DB, offset int64, size int64, withFilter func(q string) (*sqlx.NamedStmt, error)) (*sqlx.NamedStmt, *sqlx.NamedStmt, error) {
	productAttrs := map[string]string{
		"prod-weight":        "product_unit_weight",
		"prod-material":      "product_material",
		"prod-hardness":      "product_hardness",
		"prod-perform-level": "product_perform_level",
		"prod-surface-treat": "product_surface_treat",
	}

	baseTableSelect := map[string]string{
		"plan_no":            "fty_plan_package.plan_no",
		"order_no":           "fty_plan_package.order_no",
		"prod_name":          "fty_plan_package.prod_name",
		"prod_sid":           "fty_plan_package.prod_sid",
		"cust_material_no":   "fty_plan_package.cust_material_no",
		"customer_sid":       "fty_customer.sid",
		"cust_short_name":    "fty_customer.cust_short_name",
		"product_spec":       "fty_product.product_spec",
		"position":           "fty_warehouse.warehouse_name",
		"warehouse_sid":      "stock.warehouse_sid",
		"delivery_no":        "fty_delivery.delivery_no",
		"plan_package_no":    "fty_plan_package.batch_no",
		"operation_time":     "stock.create_time",
		"operator":           "stock.handle_person",
		"stock_type":         "stock.stock_type",
		"plan_package_sid":   "fty_plan_package.sid",
		"product_amount":     "fty_plan_package.product_amount",
		"warehouse_name":     "r_warehouse.warehouse_name",
		"warehouse_root_sid": "r_warehouse.sid",
	}

	countSelect := map[string]string{
		"stock_package_amount": "COUNT(plan_package_sid)",
		"stock_unit_amount":    "SUM(product_amount)",
	}

	// query string
	q := fmt.Sprintf(`
SELECT t.*,
%s
FROM (
SELECT %s, %s
	FROM ((SELECT plan_package_sid, create_time, handle_person, 'in' as stock_type, warehouse_sid, null as invoice_sid
       FROM fty_stock_in)
      UNION
      (SELECT plan_package_sid,
              create_time,
              handle_person,
              'out'                                                                  as stock_type,
              (SELECT fty_stock_in.warehouse_sid as warehouse_sid
               FROM fty_stock_in
               WHERE fty_stock_in.plan_package_sid = fty_stock_out.plan_package_sid) as warehouse_sid,
              invoice_sid
       FROM fty_stock_out)) stock
             LEFT JOIN fty_plan_package ON stock.plan_package_sid = fty_plan_package.sid
             LEFT JOIN fty_warehouse ON stock.warehouse_sid = fty_warehouse.sid
             LEFT JOIN fty_warehouse AS m_warehouse ON m_warehouse.sid = fty_warehouse.parent_sid
             LEFT JOIN fty_warehouse AS r_warehouse ON r_warehouse.sid = m_warehouse.parent_sid
             LEFT JOIN fty_delivery ON fty_delivery.sid = stock.invoice_sid
             LEFT JOIN fty_product ON fty_product.sid = fty_plan_package.prod_sid
             LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
             LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid %s 
    ) AS t __WHERE__ 
GROUP BY prod_sid, customer_sid, warehouse_sid, operation_time ORDER BY operation_time DESC
LIMIT %d,%d`,
		SqlMapToString(countSelect),
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs), offset, size)

	// count query string
	cq := fmt.Sprintf(`
SELECT COUNT(F.k) FROM (SELECT CONCAT(customer_sid, warehouse_sid, operation_time) as k
FROM (
SELECT %s, %s
	FROM ((SELECT plan_package_sid, create_time, handle_person, 'in' as stock_type, warehouse_sid, null as invoice_sid
       FROM fty_stock_in)
      UNION
      (SELECT plan_package_sid,
              create_time,
              handle_person,
              'out'                                                                  as stock_type,
              (SELECT fty_stock_in.warehouse_sid as warehouse_sid
               FROM fty_stock_in
               WHERE fty_stock_in.plan_package_sid = fty_stock_out.plan_package_sid) as warehouse_sid,
              invoice_sid
       FROM fty_stock_out)) stock
             LEFT JOIN fty_plan_package ON stock.plan_package_sid = fty_plan_package.sid
             LEFT JOIN fty_warehouse ON stock.warehouse_sid = fty_warehouse.sid
             LEFT JOIN fty_warehouse AS m_warehouse ON m_warehouse.sid = fty_warehouse.parent_sid
             LEFT JOIN fty_warehouse AS r_warehouse ON r_warehouse.sid = m_warehouse.parent_sid
             LEFT JOIN fty_delivery ON fty_delivery.sid = stock.invoice_sid
             LEFT JOIN fty_product ON fty_product.sid = fty_plan_package.prod_sid
             LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
             LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid %s) AS t
    __WHERE__ 
GROUP BY prod_sid, customer_sid, warehouse_sid, operation_time) AS F`,
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs))

	queryStmt, err := withFilter(replaceSpaceString(q))
	if err != nil {
		return nil, nil, err
	}

	countQueryStmt, err := withFilter(replaceSpaceString(cq))
	if err != nil {
		return nil, nil, err
	}

	return queryStmt, countQueryStmt, nil
}

func GetStockDistribution(db *sqlx.DB, offset int64, size int64, withFilter func(q string) (*sqlx.NamedStmt, error)) (*sqlx.NamedStmt, *sqlx.NamedStmt, error) {
	productAttrs := map[string]string{
		"prod-weight":        "product_unit_weight",
		"prod-material":      "product_material",
		"prod-hardness":      "product_hardness",
		"prod-perform-level": "product_perform_level",
		"prod-surface-treat": "product_surface_treat",
	}

	baseTableSelect := map[string]string{
		"plan_no":            "fty_plan_package.plan_no",
		"order_no":           "fty_plan_package.order_no",
		"prod_name":          "fty_plan_package.prod_name",
		"prod_sid":           "fty_plan_package.prod_sid",
		"cust_material_no":   "fty_plan_package.cust_material_no",
		"customer_sid":       "fty_customer.sid",
		"cust_short_name":    "fty_customer.cust_short_name",
		"product_spec":       "fty_product.product_spec",
		"position":           "fty_warehouse.warehouse_name",
		"warehouse_sid":      "fty_stock_in.warehouse_sid",
		"plan_package_no":    "fty_plan_package.batch_no",
		"warehouse_name":     "r_warehouse.warehouse_name",
		"warehouse_root_sid": "r_warehouse.sid",
	}

	countSelect := map[string]string{
		"stock_package_amount": `
(SELECT (count(fty_stock_in.sid) - count(fty_stock_out.sid)) t
   FROM fty_plan_package
          RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
          LEFT JOIN fty_stock_out ON fty_stock_out.plan_package_sid = fty_plan_package.sid
          LEFT JOIN fty_warehouse ON fty_warehouse.sid = fty_stock_in.warehouse_sid
          LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
          LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
   WHERE fty_plan_package.prod_sid = t.prod_sid AND fty_customer.sid = t.customer_sid AND fty_warehouse.sid = t.warehouse_sid
         AND (CASE
                 WHEN fty_plan_package.batch_no IS NOT NULL THEN
                   fty_plan_package.batch_no = t.plan_package_no
                 ELSE
                   fty_plan_package.batch_no is null
          END)
   GROUP BY fty_plan_package.prod_sid)`,
		"stock_unit_amount": `
(SELECT SUM(fty_plan_package.product_amount) -
	       SUM(CASE WHEN fty_stock_out.sid IS NOT NULL THEN fty_plan_package.product_amount else 0 END)
	FROM fty_plan_package
	       RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
	       LEFT JOIN fty_stock_out ON fty_stock_out.plan_package_sid = fty_plan_package.sid
           LEFT JOIN fty_warehouse ON fty_warehouse.sid = fty_stock_in.warehouse_sid
           LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
		   LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
	WHERE fty_plan_package.prod_sid = t.prod_sid AND fty_customer.sid = t.customer_sid AND fty_warehouse.sid = t.warehouse_sid
          AND (CASE
                 WHEN fty_plan_package.batch_no IS NOT NULL THEN
                   fty_plan_package.batch_no = t.plan_package_no
                 ELSE
                   fty_plan_package.batch_no is null
          END)
	GROUP BY fty_plan_package.prod_sid)`,
	}

	// query string
	q := fmt.Sprintf(`
SELECT *,
%s
FROM (
SELECT %s, %s
	FROM fty_product
		RIGHT JOIN fty_plan_package ON fty_product.sid = fty_plan_package.prod_sid
		RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
		LEFT JOIN fty_warehouse ON fty_warehouse.sid = fty_stock_in.warehouse_sid
        LEFT JOIN fty_warehouse AS m_warehouse ON m_warehouse.sid = fty_warehouse.parent_sid
        LEFT JOIN fty_warehouse AS r_warehouse ON r_warehouse.sid = m_warehouse.parent_sid
		LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
		LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
		%s GROUP BY fty_product.sid, fty_warehouse.sid, fty_customer.sid, fty_plan_package.batch_no) AS t __WHERE__ LIMIT %d,%d`,
		SqlMapToString(countSelect),
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs), offset, size)

	// count query string
	cq := fmt.Sprintf(`
SELECT COUNT(t.prod_sid)
FROM (
SELECT %s, %s
	FROM fty_product
		RIGHT JOIN fty_plan_package ON fty_product.sid = fty_plan_package.prod_sid
		RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
		LEFT JOIN fty_warehouse ON fty_warehouse.sid = fty_stock_in.warehouse_sid
        LEFT JOIN fty_warehouse AS m_warehouse ON m_warehouse.sid = fty_warehouse.parent_sid
        LEFT JOIN fty_warehouse AS r_warehouse ON r_warehouse.sid = m_warehouse.parent_sid
		LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
		LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
		%s GROUP BY fty_product.sid, fty_warehouse.sid, fty_customer.sid, fty_plan_package.batch_no) AS t __WHERE__`,
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs))

	queryStmt, err := withFilter(replaceSpaceString(q))
	if err != nil {
		return nil, nil, err
	}

	countQueryStmt, err := withFilter(replaceSpaceString(cq))
	if err != nil {
		return nil, nil, err
	}

	return queryStmt, countQueryStmt, nil
}

func GetStockAllProduct(db *sqlx.DB, offset int64, size int64, withFilter func(q string) (*sqlx.NamedStmt, error)) (*sqlx.NamedStmt, *sqlx.NamedStmt, error) {
	productAttrs := map[string]string{
		"prod-weight":        "product_unit_weight",
		"prod-material":      "product_material",
		"prod-hardness":      "product_hardness",
		"prod-perform-level": "product_perform_level",
		"prod-surface-treat": "product_surface_treat",
	}

	baseTableSelect := map[string]string{
		"plan_no":          "fty_plan_package.plan_no",
		"order_no":         "fty_plan_package.order_no",
		"prod_name":        "fty_plan_package.prod_name",
		"prod_sid":         "fty_plan_package.prod_sid",
		"cust_material_no": "fty_plan_package.cust_material_no",
		"customer_sid":     "fty_customer.sid",
		"cust_short_name":  "fty_customer.cust_short_name",
		"product_spec":     "fty_product.product_spec",
	}

	countSelect := map[string]string{
		"stock_package_amount": `
(SELECT (count(fty_stock_in.sid) - count(fty_stock_out.sid)) t
   FROM fty_plan_package
          RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
          LEFT JOIN fty_stock_out ON fty_stock_out.plan_package_sid = fty_plan_package.sid
   WHERE fty_plan_package.prod_sid = t.prod_sid
   GROUP BY fty_plan_package.prod_sid)`,
		"stock_unit_amount": `
(SELECT SUM(fty_plan_package.product_amount) -
	       SUM(CASE WHEN fty_stock_out.sid IS NOT NULL THEN fty_plan_package.product_amount else 0 END)
	FROM fty_plan_package
	       RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
	       LEFT JOIN fty_stock_out ON fty_stock_out.plan_package_sid = fty_plan_package.sid
	WHERE fty_plan_package.prod_sid = t.prod_sid
	GROUP BY fty_plan_package.prod_sid)`,
	}

	// query string
	q := fmt.Sprintf(`
SELECT *,
%s
FROM (
SELECT %s, %s
	FROM fty_product
		RIGHT JOIN fty_plan_package ON fty_product.sid = fty_plan_package.prod_sid
		RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
		LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
		LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
		%s GROUP BY fty_product.sid, fty_customer.sid) AS t __WHERE__ LIMIT %d,%d`,
		SqlMapToString(countSelect),
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs), offset, size)

	// count query string
	cq := fmt.Sprintf(`
SELECT COUNT(t.prod_sid)
FROM (
SELECT %s, %s
	FROM fty_product
		RIGHT JOIN fty_plan_package ON fty_product.sid = fty_plan_package.prod_sid
		RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
		LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
		LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
		%s GROUP BY fty_product.sid, fty_customer.sid) AS t __WHERE__`,
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs))

	queryStmt, err := withFilter(replaceSpaceString(q))
	if err != nil {
		return nil, nil, err
	}

	countQueryStmt, err := withFilter(replaceSpaceString(cq))
	if err != nil {
		return nil, nil, err
	}

	return queryStmt, countQueryStmt, nil
}

func GetStockOrderItemUnshipped(db *sqlx.DB, offset int64, size int64, withFilter func(q string) (*sqlx.NamedStmt, error)) (*sqlx.NamedStmt, *sqlx.NamedStmt, error) {
	productAttrs := map[string]string{
		"prod-surface-treat": "product_surface_treat",
	}

	baseTableSelect := map[string]string{
		"plan_no":           "fty_plan_package.plan_no",
		"order_no":          "fty_plan_package.order_no",
		"prod_name":         "fty_plan_package.prod_name",
		"prod_sid":          "fty_plan_package.prod_sid",
		"cust_material_no":  "fty_plan_package.cust_material_no",
		"customer_sid":      "fty_customer.sid",
		"cust_short_name":   "fty_customer.cust_short_name",
		"product_spec":      "fty_product.product_spec",
		"item_sid":          "fty_order_item.sid",
		"item_order_amount": "fty_order_item.amount",
		"delivery_date":     "fty_order_item.delivery_date",
		"remaining_days":    "(TIMESTAMPDIFF(DAY,NOW(),fty_order_item.delivery_date))",
	}

	countSelect := map[string]string{
		"remaining_stock": `(SELECT (fty_order_item.amount - SUM(fty_delivery_packing.product_amount)) 
FROM fty_order_item LEFT JOIN fty_delivery_packing ON fty_delivery_packing.product_sid = fty_order_item.product_sid 
WHERE fty_order_item.sid = t.item_sid)`,
		"warehouse_stock_total": `(SELECT SUM(fty_plan_package.product_amount) -  SUM(CASE WHEN fty_stock_out.sid IS NOT NULL THEN fty_plan_package.product_amount else 0 END)
FROM fty_plan_package 
RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid= fty_plan_package.sid
LEFT JOIN fty_stock_out ON fty_stock_out.plan_package_sid= fty_plan_package.sid
WHERE fty_plan_package.prod_sid= t.prod_sid
GROUP BY fty_plan_package.prod_sid)`,
	}

	// query string
	q := fmt.Sprintf(`
SELECT *,
%s
FROM (
SELECT %s, %s
	FROM fty_order_item 
        LEFT JOIN fty_product ON fty_product.sid = fty_order_item.product_sid
		RIGHT JOIN fty_plan_package ON fty_product.sid = fty_plan_package.prod_sid
		RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
		LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
		LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
		%s GROUP BY fty_order_item.sid, fty_product.sid, fty_customer.sid) AS t __WHERE__ LIMIT %d,%d`,
		SqlMapToString(countSelect),
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs), offset, size)

	// count query string
	cq := fmt.Sprintf(`
SELECT COUNT(t.item_sid)
FROM (
SELECT %s, %s
	FROM fty_order_item 
        LEFT JOIN fty_product ON fty_product.sid = fty_order_item.product_sid
		RIGHT JOIN fty_plan_package ON fty_product.sid = fty_plan_package.prod_sid
		RIGHT JOIN fty_stock_in ON fty_stock_in.plan_package_sid = fty_plan_package.sid
		LEFT JOIN fty_product_cust_attr ON fty_product_cust_attr.sid = fty_plan_package.product_cust_attr_sid
		LEFT JOIN fty_customer ON fty_customer.sid = fty_product_cust_attr.cust_sid
		%s GROUP BY fty_order_item.sid, fty_product.sid, fty_customer.sid) AS t __WHERE__`,
		SqlMapToString(baseTableSelect),
		ProductAttrsToSelect(productAttrs),
		ProductAttrsToJoinInStat(db, productAttrs))

	queryStmt, err := withFilter(replaceSpaceString(q))
	if err != nil {
		return nil, nil, err
	}

	countQueryStmt, err := withFilter(replaceSpaceString(cq))
	if err != nil {
		return nil, nil, err
	}

	return queryStmt, countQueryStmt, nil
}
