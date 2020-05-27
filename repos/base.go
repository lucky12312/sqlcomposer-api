package repos

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
	"reflect"
	"regexp"
	"strings"
)

type Operator string
type LogicOperator string

const (
	Equal          Operator = "="
	NotEqual                = "<>"
	Greater                 = ">"
	Less                    = "<"
	GreaterOrEqual          = ">="
	LessOrEqual             = "<="
	StartsWith              = "starts_with"
	Contains                = "contains"
	EndsWith                = "ends_with"
	In                      = "in"
	NotIn                   = "not_in"
	Between                 = "between"
	NotBetween              = "not_between"
	IsNull                  = "is_null"
	IsNotNull               = "is_not_null"
)

const (
	AND LogicOperator = "AND"
	OR                = "OR"
)

type TokenReplacer interface {
	TokenReplace(ctx map[string]interface{}) string
}

type SqlCompositionFields map[string]SqlCompositionFieldGroup

type SqlCompositionFieldGroup []SqlCompositionField

func (group SqlCompositionFieldGroup) TokenReplace(ctx map[string]interface{}) string {
	var res []string
	for _, field := range group {
		res = append(res, fmt.Sprintf("%s AS %s", field.Expr, field.Name))
	}

	return strings.Join(res, ", ")
}

type SqlCompositionField struct {
	Name string `yaml:"name"`
	Expr string `yaml:"expr"`
}

type TokenParam struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type TokenDefinition struct {
	Params []TokenParam `yaml:"params,omitempty"`
}

type SqlApiDoc struct {
	Info struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
		Path    string `yaml:"path"`
		Db     string `yaml:"db"`
	} `yaml:"info"`
	Composition struct {
		Fields  SqlCompositionFields       `yaml:"fields"`
		Tokens  map[string]TokenDefinition `yaml:"tokens,omitempty"`
		Subject string                     `yaml:"subject"`
		Total   string                     `yaml:"total,omitempty"`
	} `yaml:"composition"`
}

type SqlBuilderContext map[string]interface{}

// SqlBuilder be responsible for build sql from yaml config
type SqlBuilder struct {
	DB         *sqlx.DB
	Doc        SqlApiDoc
	Context    SqlBuilderContext
	conditions *FilterStmt
	limit      *SqlLimit
}

func NewSqlBuilder(db *sqlx.DB, yamlFile []byte) (*SqlBuilder, error) {
	doc := SqlApiDoc{}

	err := yaml.Unmarshal(yamlFile, &doc)

	if err != nil {
		return nil, errors.Wrap(err, "Construct SqlBuilder failure")
	}

	return &SqlBuilder{
		DB:         db,
		Doc:        doc,
		Context:    make(SqlBuilderContext),
		conditions: new(FilterStmt),
		limit:      &SqlLimit{0, 10},
	}, nil
}

func (sc *SqlBuilder) RegisterToken(name string, gen func(params []TokenParam) TokenReplacer) error {
	if td, ok := sc.Doc.Composition.Tokens[name]; ok {
		sc.Context[name] = gen(td.Params)
		return nil
	}
	return errors.New(fmt.Sprintf("token %s not defined", name))
}

func (sc *SqlBuilder) AndConditions(c *FilterStmt) *SqlBuilder {
	combined := Combine(AND, *sc.conditions, *c)
	sc.conditions = &combined
	return sc
}

func (sc *SqlBuilder) OrConditions(c *FilterStmt) *SqlBuilder {
	combined := Combine(OR, *sc.conditions, *c)
	sc.conditions = &combined
	return sc
}

func (sc *SqlBuilder) Limit(offset int64, size int64) *SqlBuilder {
	sc.limit.Offset = offset
	sc.limit.Size = size
	return sc
}

func (sc *SqlBuilder) compose(s string) (string, error) {
	ctx := map[string]interface{}{
		"where": *sc.conditions,
		"limit": *sc.limit,
	}

	// fields context process
	for k, g := range sc.Doc.Composition.Fields {
		ctx["fields."+k] = g
	}

	for k, v := range sc.Context {
		ctx[k] = v
	}

	return tokenReplace(s, ctx)
}

// Build query statement
func (sc *SqlBuilder) BuildQuery() (stmt *sqlx.NamedStmt, err error) {
	subject, err := sc.compose(sc.Doc.Composition.Subject)

	if err != nil {
		return stmt, errors.Wrap(err, "subject sql token replace fail")
	}

	return sc.DB.PrepareNamed(subject)
}

// Build total query statement
func (sc *SqlBuilder) BuildTotalQuery() (stmt *sqlx.NamedStmt, err error) {
	total, err := sc.compose(sc.Doc.Composition.Total)

	if err != nil {
		return stmt, errors.Wrap(err, "subject sql token replace fail")
	}

	return sc.DB.PrepareNamed(total)
}

type Filter struct {
	Val  interface{}
	Op   Operator
	Attr string
}

type FilterGroup struct {
	LogicOp LogicOperator
	Filters []*Filter
}

type FilterStmt struct {
	Clause string
	Arg    map[string]interface{}
}

func (fs FilterStmt) IsEmpty() bool {
	return fs.Clause == ""
}

// Implement token replacer
func (fs FilterStmt) TokenReplace(ctx map[string]interface{}) string {
	if !fs.IsEmpty() {
		return fmt.Sprintf("WHERE %s", fs.Clause)
	}

	return ""
}

type SqlLimit struct {
	Offset int64
	Size   int64
}

// Implement token replacer
func (limit SqlLimit) TokenReplace(ctx map[string]interface{}) string {
	return fmt.Sprintf("LIMIT %d, %d", limit.Offset, limit.Size)
}

//
// Condition handlers
//

// Handle filters to filters statement
func Conditions(f *[]Filter, op LogicOperator) (stmt FilterStmt, err error) {
	var (
		conditions []string
	)

	conditions = []string{}
	stmt.Arg = map[string]interface{}{}

	for _, value := range *f {
		var str strings.Builder

		paramsAttr := strings.Replace(value.Attr, ".", "_", -1)

		switch value.Op {
		case StartsWith:
			str.WriteString(fmt.Sprintf("%s LIKE :%s", value.Attr, paramsAttr))

			err = likeParamsProcess(value.Val, paramsAttr, value.Op, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}
		case Contains:
			str.WriteString(fmt.Sprintf("%s LIKE :%s", value.Attr, paramsAttr))

			err = likeParamsProcess(value.Val, paramsAttr, value.Op, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}
		case EndsWith:
			str.WriteString(fmt.Sprintf("%s LIKE :%s", value.Attr, paramsAttr))

			err = likeParamsProcess(value.Val, paramsAttr, value.Op, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}

			break
		case In:
			str.WriteString(fmt.Sprintf("%s IN(:%s)", value.Attr, paramsAttr))
			stmt.Arg[paramsAttr] = value.Val
			break
		case NotIn:
			str.WriteString(fmt.Sprintf("%s NOT IN(:%s)", value.Attr, paramsAttr))
			stmt.Arg[paramsAttr] = value.Val
			break
		case Between:
			str.WriteString(fmt.Sprintf("%s > :%s AND %s < :%s",
				value.Attr, paramsAttr+"_1", value.Attr, paramsAttr+"_2"))

			err = betweenParamsProcess(value.Val, paramsAttr, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}

			break
		case NotBetween:
			str.WriteString(fmt.Sprintf("%s < :%s AND %s > :%s",
				value.Attr, paramsAttr+"_1", value.Attr, paramsAttr+"_2"))

			err = betweenParamsProcess(value.Val, paramsAttr, stmt.Arg)
			if err != nil {
				return stmt, errors.Wrap(err, "arg build failure")
			}
			break
		case IsNull:
			str.WriteString(fmt.Sprintf("%s IS NULL", value.Attr))
			break
		case IsNotNull:
			str.WriteString(fmt.Sprintf("%s IS NOT NULL", value.Attr))
			break
		default:
			str.WriteString(fmt.Sprintf("%s %s :%s", value.Attr, value.Op, paramsAttr))
			stmt.Arg[paramsAttr] = value.Val
		}

		conditions = append(conditions, str.String())
	}

	stmt.Clause = strings.Join(conditions, fmt.Sprintf(" %s ", op))
	return stmt, nil
}

func WhereOr(f *[]Filter) (stmt FilterStmt, err error) {
	return Conditions(f, OR)
}

func WhereAnd(f *[]Filter) (stmt FilterStmt, err error) {
	return Conditions(f, AND)
}

func CombineOr(stmts ...FilterStmt) (stmt FilterStmt) {
	return Combine(OR, stmts...)
}

func CombineAnd(stmts ...FilterStmt) (stmt FilterStmt) {
	return Combine(AND, stmts...)
}

// Combine two or more filter statement to one
func Combine(op LogicOperator, stmts ...FilterStmt) (stmt FilterStmt) {
	var clauses []string
	stmt.Arg = map[string]interface{}{}

	for _, s := range stmts {
		if s.Clause != "" {
			clauses = append(clauses, fmt.Sprintf("(%s)", s.Clause))

			for k, sa := range s.Arg {
				stmt.Arg[k] = sa
			}
		}
	}

	stmt.Clause = strings.Join(clauses, fmt.Sprintf(" %s ", op))

	return stmt
}

// Helper func for process the between params
func betweenParamsProcess(v interface{}, attr string, params map[string]interface{}) error {
	s := reflect.ValueOf(v)

	if s.Kind() != reflect.Slice {
		return errors.New("between operator value must be slice type")
	}

	if s.Len() != 2 {
		return errors.New("between operator required two value")
	}

	k := s.Index(0).Kind()

	if k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64 {
		params[attr+"_1"] = s.Index(0).Int()
		params[attr+"_2"] = s.Index(1).Int()
	}

	if k == reflect.Float32 || k == reflect.Float64 {
		params[attr+"_1"] = s.Index(0).Float()
		params[attr+"_2"] = s.Index(1).Float()
	}

	if k == reflect.String {
		params[attr+"_1"] = s.Index(0).String()
		params[attr+"_2"] = s.Index(1).String()
	}

	return nil
}

// Helper func for process the like params
func likeParamsProcess(v interface{}, attr string, op Operator, params map[string]interface{}) error {
	s := reflect.ValueOf(v)
	if s.Kind() != reflect.String {
		return errors.New("like operator value must be string type")
	}

	if op == StartsWith {
		params[attr] = v.(string) + "%"
	}

	if op == EndsWith {
		params[attr] = "%" + v.(string)
	}

	if op == Contains {
		params[attr] = "%" + v.(string) + "%"
	}

	return nil
}

//
// Filter pipeline
//
type Expander interface {
	Expand(origFilter Filter) (FilterStmt, error)
}

type FilterPipeline struct {
	Attr      string
	CombineOp LogicOperator
	Expander  Expander
}

// High order filter handler that can with pipelines, pipeline definition could implement custom behaviors to process
// complex filter logic
func FilterToWhereAnd(filters *[]Filter, pipelines ...FilterPipeline) (stmt FilterStmt, err error) {
	var restFilters []Filter

	for _, f := range *filters {
		contains := false
		for _, p := range pipelines {
			if f.Attr == p.Attr {
				contains = true
			}
		}

		if !contains {
			restFilters = append(restFilters, f)
		}
	}

	stmt, err = WhereAnd(&restFilters)

	for _, f := range *filters {
		for _, p := range pipelines {
			if f.Attr == p.Attr {
				subFilterStmt, err := p.Expander.Expand(f)

				if err != nil {
					return stmt, err
				}

				stmt = Combine(p.CombineOp, subFilterStmt, stmt)
			}
		}
	}

	return stmt, err
}

//
// Token replace
//
func tokenReplace(s string, ctx map[string]interface{}) (rs string, err error) {
	// collect all token placeholders on the string
	tps := CollectTokenPlaceholder(s)

	// no token need replace
	if len(tps) == 0 {
		return s, nil
	}

	rs = s
	for _, placeholder := range tps {
		if tr, ok := ctx[placeholder[1]]; ok {
			// tr is string
			if rt := reflect.TypeOf(tr); rt.Kind() == reflect.String {
				rs = strings.Replace(rs, placeholder[0], tr.(string), 1)
			} else {
				replacer, ok := tr.(TokenReplacer)

				if !ok {
					return rs, errors.New(fmt.Sprintf("placeholder %s in context must implemented TokenReplacer", placeholder[0]))
				}

				rs = strings.Replace(rs, placeholder[0], replacer.TokenReplace(ctx), 1)
			}
		} else {
			return rs, errors.New(fmt.Sprintf("placeholder %s not definition in context", placeholder[1]))
		}
	}

	return replaceSpaceString(rs), err
}

func replaceSpaceString(s string) string {
	return strings.Replace(strings.Replace(s, "\n", " ", -1), "\t", " ", -1)
}

func CollectTokenPlaceholder(s string) (tps [][]string) {
	r := regexp.MustCompile(`%([\w.]+)`)
	return r.FindAllStringSubmatch(s, -1)
}
