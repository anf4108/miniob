%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <memory>

#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/yacc_sql.hpp"
#include "sql/parser/lex_sql.h"
#include "sql/expr/expression.h"

using namespace std;

string token_name(const char *sql_string, YYLTYPE *llocp) {
    return string(sql_string + llocp->first_column, llocp->last_column - llocp->first_column + 1);
}

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, yyscan_t scanner, ParseContext *context, const char *msg);

ArithmeticExpr *create_arithmetic_expression(ArithmeticExpr::Type type,
                                            Expression *left,
                                            Expression *right,
                                            const char *sql_string,
                                            YYLTYPE *llocp,
                                            ParseContext *context) {
    ArithmeticExpr *expr = new ArithmeticExpr(type, left, right);
    // when facing problems, I will add more code here. 
    // 或许可以考虑unique_ptr包装所有的函数?
    context->add_object(expr);
    context->remove_object(left);
    context->remove_object(right);
    expr->set_name(token_name(sql_string, llocp));
    return expr;
}
UnboundAggregateExpr *create_aggregate_expression(
    const char *aggregate_name,
    std::unique_ptr<Expression> child,  // 改为接受unique_ptr
    const char *sql_string,
    YYLTYPE *llocp,
    ParseContext *context) 
{
    UnboundAggregateExpr *expr = new UnboundAggregateExpr(aggregate_name, child.release());
    context->add_object(expr);
    context->remove_object(child.get());  // 移除child的所有权
    expr->set_name(token_name(sql_string, llocp));
    return expr;
}

UnboundAggregateExpr *create_aggregate_expression(const char *aggregate_name,
                                                 Expression *child,
                                                 const char *sql_string,
                                                 YYLTYPE *llocp,
                                                 ParseContext *context) {
    UnboundAggregateExpr *expr = new UnboundAggregateExpr(aggregate_name, child);
    context->add_object(expr);
    context->remove_object(child);
    expr->set_name(token_name(sql_string, llocp));
    return expr;
}
%}

%define api.pure full
%define parse.error verbose
%locations
%lex-param { yyscan_t scanner }
%parse-param { const char * sql_string }
%parse-param { ParsedSqlResult * sql_result }
%parse-param { void * scanner }
%parse-param { ParseContext * context }

%code requires {
    struct ParseContext;
}

//标识tokens
%token  SEMICOLON
        BY
        CREATE
        DROP
        GROUP
        TABLE
        TABLES
        INDEX
        CALC
        SELECT
        DESC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE
        LBRACE
        RBRACE
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        FLOAT_T
        DATE_T // date
        VECTOR_T
        HELP
        EXIT
        DOT //QUOTE
        NOT
        NULL_T
        INTO
        VALUES
        FROM
        WHERE
        AND
        SET
        ON
        IN
        EXISTS
        LOAD
        DATA
        INFILE
        EXPLAIN
        STORAGE
        FORMAT
        AS
        EQ
        LT
        GT
        LE
        GE
        NE
        IS_TOKEN
        LIKE
        SYS_LENGTH
        SYS_ROUND
        SYS_DATE_FORMAT

/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *                            sql_node;
  ConditionSqlNode *                         condition;
  Value *                                    value;
  enum CompOp                                comp;
  enum SysFuncType                           functype;
  RelAttrSqlNode *                           rel_attr;
  vector<AttrInfoSqlNode> *                  attr_infos;
  AttrInfoSqlNode *                          attr_info;
  Expression *                               expression;
  RelationSqlNode *                          relation;
  vector<unique_ptr<Expression>> *           expression_list;
  vector<UpdateInfoNode>*               update_info_list;
  vector<Value> *                            value_list;
  vector<ConditionSqlNode> *                 condition_list;
  vector<RelAttrSqlNode> *                   rel_attr_list;
  vector<RelationSqlNode> *                  relation_list;
  char *                                     cstring;
  int                                        number;
  float                                      floats;
  bool                                       boolean;
}

%token <number> NUMBER
%token <floats> FLOAT
%token <cstring> ID
%token <cstring> SSS
%token <cstring> DATE
//非终结符

/** type 定义了各种解析后的结果输出的是什么类型。类型对应了 union 中的定义的成员变量名称 **/
%type <number>              type
%type <condition>           condition
%type <value>               value
%type <number>              number
%type <functype>            sys_func_type
%type <relation>            relation
%type <cstring>             alias
%type <comp>                comp_op
%type <comp>                exists_op
%type <rel_attr>            rel_attr
%type <attr_infos>          attr_def_list
%type <attr_info>           attr_def
%type <value_list>          value_list
%type <condition_list>      where
%type <condition_list>      condition_list
%type <cstring>             storage_format
%type <relation_list>       rel_list
%type <expression>          expression
%type <expression>          aggregate_func
%type <expression>          sys_func
%type <expression_list>     expression_list
%type <expression_list>     group_by
%type <expression>          sub_query_expr
%type <update_info_list>    update_list
%type <sql_node>            calc_stmt
%type <sql_node>            select_stmt
%type <sql_node>            insert_stmt
%type <sql_node>            update_stmt
%type <sql_node>            delete_stmt
%type <sql_node>            create_table_stmt
%type <sql_node>            drop_table_stmt
%type <sql_node>            show_tables_stmt
%type <sql_node>            desc_table_stmt
%type <sql_node>            create_index_stmt
%type <sql_node>            drop_index_stmt
%type <sql_node>            sync_stmt
%type <sql_node>            begin_stmt
%type <sql_node>            commit_stmt
%type <sql_node>            rollback_stmt
%type <sql_node>            load_data_stmt
%type <sql_node>            explain_stmt
%type <sql_node>            set_variable_stmt
%type <sql_node>            help_stmt
%type <sql_node>            exit_stmt
%type <sql_node>            command_wrapper
// commands should be a list but I use a single command instead
%type <sql_node>            commands
%type <boolean>            null_option

%left '+' '-'
%left '*' '/'
%nonassoc UMINUS
%%

commands: command_wrapper opt_semicolon  //commands or sqls. parser starts here.
  {
    unique_ptr<ParsedSqlNode> sql_node = unique_ptr<ParsedSqlNode>($1);
    sql_result->add_sql_node(std::move(sql_node));
    context->remove_all_objects();
  }
  ;

command_wrapper:
    calc_stmt
  | select_stmt
  | insert_stmt
  | update_stmt
  | delete_stmt
  | create_table_stmt
  | drop_table_stmt
  | show_tables_stmt
  | desc_table_stmt
  | create_index_stmt
  | drop_index_stmt
  | sync_stmt
  | begin_stmt
  | commit_stmt
  | rollback_stmt
  | load_data_stmt
  | explain_stmt
  | set_variable_stmt
  | help_stmt
  | exit_stmt
    ;

exit_stmt:      
    EXIT {
      (void)yynerrs;  // 这么写为了消除yynerrs未使用的告警。如果你有更好的方法欢迎提PR
      $$ = new ParsedSqlNode(SCF_EXIT);
      context->add_object($$);
    };

help_stmt:
    HELP {
      $$ = new ParsedSqlNode(SCF_HELP);
      context->add_object($$);
    };

sync_stmt:
    SYNC {
      $$ = new ParsedSqlNode(SCF_SYNC);
      context->add_object($$);
    }
    ;

begin_stmt:
    TRX_BEGIN  {
      $$ = new ParsedSqlNode(SCF_BEGIN);
      context->add_object($$);
    }
    ;

commit_stmt:
    TRX_COMMIT {
      $$ = new ParsedSqlNode(SCF_COMMIT);
      context->add_object($$);
    }
    ;

rollback_stmt:
    TRX_ROLLBACK  {
      $$ = new ParsedSqlNode(SCF_ROLLBACK);
      context->add_object($$);
    }
    ;

drop_table_stmt:    /*drop table 语句的语法解析树*/
    DROP TABLE ID {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      context->add_object($$);
      $$->drop_table.relation_name = $3;
    };

show_tables_stmt:
    SHOW TABLES {
      $$ = new ParsedSqlNode(SCF_SHOW_TABLES);
      context->add_object($$);
    }
    ;

desc_table_stmt:
    DESC ID  {
      $$ = new ParsedSqlNode(SCF_DESC_TABLE);
      context->add_object($$);
      $$->desc_table.relation_name = $2;

    }
    ;

create_index_stmt:    /*create index 语句的语法解析树*/
    CREATE INDEX ID ON ID LBRACE ID RBRACE
    {
      $$ = new ParsedSqlNode(SCF_CREATE_INDEX);
      context->add_object($$);
      CreateIndexSqlNode &create_index = $$->create_index;
      create_index.index_name = $3;
      create_index.relation_name = $5;
      create_index.attribute_name = $7;
    }
    ;

drop_index_stmt:      /*drop index 语句的语法解析树*/
    DROP INDEX ID ON ID
    {
      $$ = new ParsedSqlNode(SCF_DROP_INDEX);
      context->add_object($$);
      $$->drop_index.index_name = $3;
      $$->drop_index.relation_name = $5;
    }
    ;
create_table_stmt:    /*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE storage_format
    {
      // $$ 表示返回值
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      context->add_object($$);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;
      // 传递的是指针, 不进行拷贝:
      //free($3);

      vector<AttrInfoSqlNode> *src_attrs = $6;

      if (src_attrs != nullptr) {
        create_table.attr_infos.swap(*src_attrs);
        delete src_attrs;
      }
      create_table.attr_infos.emplace_back(*$5);
      reverse(create_table.attr_infos.begin(), create_table.attr_infos.end());
      // debug here
      // for (auto &attr : create_table.attr_infos) {
      //   printf("DEBUG: attr name: %s, type: %s, length: %d\n", attr.name.c_str(), attr_type_to_string(attr.type), (int)attr.length);
      // }
      // debug end
      delete $5;
      if ($8 != nullptr) {
        create_table.storage_format = $8;
      }
    }
    ;
attr_def_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA attr_def attr_def_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new vector<AttrInfoSqlNode>;
        context->add_object($$);
      }
      $$->emplace_back(*$2);
      context->remove_object($2);
      delete $2;
    }
    ;
    
attr_def:
    // number 表示长度
    ID type LBRACE number RBRACE null_option
    {
      $$ = new AttrInfoSqlNode;
      context->add_object($$);
      $$->type = (AttrType)$2;
      $$->name = $1; // std::string 复制 $1(为char*) 的内容
      $$->length = $4;
      $$->nullable = $6;
      // Is this necessary? 何时free
      // free($1);
    }
    | ID type null_option
    {
      $$ = new AttrInfoSqlNode;
      context->add_object($$);
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
      $$->nullable = $3;
    }
    ;

null_option:
    /* empty */
    {
      $$ = true;
    }
    | NULL_T {
      $$ = true;
    }
    | NOT NULL_T {
      $$ = false;
    }
    ;

number:
    NUMBER {$$ = $1;}
    ;
type:
    INT_T      { $$ = static_cast<int>(AttrType::INTS); }
    | STRING_T { $$ = static_cast<int>(AttrType::CHARS); }
    | FLOAT_T  { $$ = static_cast<int>(AttrType::FLOATS); }
    | VECTOR_T { $$ = static_cast<int>(AttrType::VECTORS); }
    | DATE_T   { $$ = static_cast<int>(AttrType::DATES); }
    ;
insert_stmt:        /*insert   语句的语法解析树*/
    INSERT INTO ID VALUES LBRACE value value_list RBRACE 
    {
      $$ = new ParsedSqlNode(SCF_INSERT);
      context->add_object($$);
      $$->insertion.relation_name = $3;
      if ($7 != nullptr) {
        $$->insertion.values.swap(*$7);
        delete $7;
      }
      $$->insertion.values.emplace_back(*$6);
      reverse($$->insertion.values.begin(), $$->insertion.values.end());
      delete $6;
    }
    ;

value_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA value value_list  { 
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new vector<Value>;
        context->add_object($$);
      }
      $$->emplace_back(*$2);
      context->remove_object($2);
      delete $2;
    }
    ;
value:
    NUMBER {
      LOG_DEBUG("DEBUG: reduce NUMBER");
      $$ = new Value((int)$1);
      context->add_object($$);
      @$ = @1;
    }
    | '-' NUMBER {
      $$ = new Value(-(int)$2);
      context->add_object($$);
      @$ = @2; // 将位置信息设置为 NUMBER 的位置
    }
    |FLOAT {
      $$ = new Value((float)$1);
      context->add_object($$);
      @$ = @1;
    }
    | '-' FLOAT {
      $$ = new Value(-(float)$2);
      context->add_object($$);
      @$ = @2;
    }
    |SSS {
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$ = new Value(tmp);
      context->add_object($$);
      free(tmp);
    }
    | DATE {
      // 进行重构
      char *tmp = common::substr($1,1,strlen($1)-2);
      bool is_date = true;
      $$ = new Value(tmp, is_date);
      if (!$$->is_valid_date()) {
        $$->reset();
      }
      context->add_object($$);
      free(tmp);
    }
    | NULL_T {
      $$ = new Value();
      $$->set_null();
      context->add_object($$);
    }
    ;
storage_format:
    /* empty */
    {
      $$ = nullptr;
    }
    | STORAGE FORMAT EQ ID
    {
      $$ = $4;
    }
    ;
    
delete_stmt:    /*  delete 语句的语法解析树*/
    DELETE FROM ID where 
    {
      $$ = new ParsedSqlNode(SCF_DELETE);
      context->add_object($$);
      $$->deletion.relation_name = $3;
      if ($4 != nullptr) {
        $$->deletion.conditions.swap(*$4);
        delete $4;
      }
    }
    ;
update_stmt:      /*  update 语句的语法解析树*/
    UPDATE ID SET update_list where 
    {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      context->add_object($$);
      $$->update.relation_name = $2;
      $$->update.update_infos = *$4;
      delete $4;
      if ($5 != nullptr) {
        $$->update.conditions.swap(*$5);
        delete $5;
      }
      free($2);
    }
    ;
update_list:
    ID EQ expression COMMA update_list
    {
        $$ = $5;
        $$->emplace_back(string($1), $3);
        free($1);
    }
    | ID EQ expression
    {
        $$ = new vector<UpdateInfoNode>();
        $$->emplace_back(string($1), $3);
        free($1);
    }
    ;
select_stmt:        /*  select 语句的语法解析树*/
    SELECT expression_list {
      $$ = new ParsedSqlNode(SCF_SELECT);
      context->add_object($$);
      if ($2 != nullptr) {
        $$->selection.expressions.swap(*$2);
        delete $2;
      }
    }
    | SELECT expression_list FROM rel_list where group_by
    {
      LOG_DEBUG("DEBUG: select_stmt");
      $$ = new ParsedSqlNode(SCF_SELECT);
      context->add_object($$);
      if ($2 != nullptr) {
        $$->selection.expressions.swap(*$2);
        delete $2;
      }

      if ($4 != nullptr) {
        $$->selection.relations.swap(*$4);
        delete $4;
      }

      if ($5 != nullptr) {
        $$->selection.conditions.swap(*$5);
        // 左递归，需要倒置
        reverse($$->selection.conditions.begin(), $$->selection.conditions.end());
        delete $5;
      }

      if ($6 != nullptr) {
        $$->selection.group_by.swap(*$6);
        delete $6;
      }
    }
    ;
calc_stmt:
    CALC expression_list
    {
      LOG_DEBUG("DEBUG: reduce calc_stmt");
      $$ = new ParsedSqlNode(SCF_CALC);
      context->add_object($$);
      $$->calc.expressions.swap(*$2);
      delete $2;
    }
    ;

expression_list:
    expression alias
    {
      $$ = new vector<unique_ptr<Expression>>;
      context->add_object($$);
      if ($2 != nullptr) {
        $1->set_alias($2);
      }
      $$->emplace_back($1);
      // free($2);
      context->remove_object($1);
    }
    | expression alias COMMA expression_list
    {
      if ($4 != nullptr) {
        $$ = $4;
      } else {
        $$ = new vector<unique_ptr<Expression>>;
        context->add_object($$);
      }
      if ($2 != nullptr) {
        $1->set_alias($2);
      }
      // push to front
      $$->emplace($$->begin(), $1);
      // free($2);
      context->remove_object($1);
    }
    ;
  
// 目前表达式不允许空
expression:
    expression '+' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::ADD, $1, $3, sql_string, &@$, context);
    }
    | expression '-' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::SUB, $1, $3, sql_string, &@$, context);
    }
    | expression '*' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::MUL, $1, $3, sql_string, &@$, context);
    }
    | expression '/' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::DIV, $1, $3, sql_string, &@$, context);
    }
    | LBRACE expression RBRACE {
      $$ = $2;
      $$->set_name(token_name(sql_string, &@$));
    }
    // 失去右节点
    | '-' expression %prec UMINUS {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::NEGATIVE, $2, nullptr, sql_string, &@$, context);
    }
    | value {
      $$ = new ValueExpr(*$1);  // 拷贝构造
      context->add_object($$);
      $$->set_name(token_name(sql_string, &@$));
      context->remove_object($1);
      delete $1;
    }
    | LBRACE value value_list RBRACE  {
      vector<Value> *values = $3;
      values->emplace_back(*$2);
      $$ = new ValueListExpr(*values);
      $$->set_name(token_name(sql_string, &@$));
    }
    | rel_attr {
      RelAttrSqlNode *node = $1;
      if (node->attribute_name == "*" && node->relation_name != "") {
        $$ = new StarExpr(node->relation_name.c_str());
      } else {
        $$ = new UnboundFieldExpr(node->relation_name, node->attribute_name);
      }
      context->add_object($$);
      $$->set_name(token_name(sql_string, &@$));
      context->remove_object($1);
      delete $1;
    }
    | '*' {
      $$ = new StarExpr();
      context->add_object($$);
      $$->set_name(token_name(sql_string, &@$));
    }
    // 聚合函数Reduce
    | aggregate_func
    {
      $$ = $1;
    }
    | sys_func {
      $$ = $1;
    }
    | sub_query_expr {
      $$ = $1;
    }
    // your code here
    ;

aggregate_func:
    ID LBRACE expression_list RBRACE {
      if ($3->size() != 1) {
        Expression *star = new StarExpr();
        context->add_object(star);  // 注册 StarExpr
        context->clear_object($3);  // 清除 $3 中的对象
        $$ = create_aggregate_expression("MAX", star, sql_string, &@$, context);
      } else {
        Expression *child = $3->at(0).release();  // 转移所有权
        context->remove_object(child);  // 从 context 中移除，因为它现在由 create_aggregate_expression 管理
        context->clear_object($3);  // 清除 $3 中的对象
        $$ = create_aggregate_expression($1, child, sql_string, &@$, context);
      }
    }
    | ID LBRACE RBRACE {
      Expression *star = new StarExpr();
      context->add_object(star);  // 注册 StarExpr
      $$ = create_aggregate_expression("MAX", star, sql_string, &@$, context);
    }
    ;

sys_func_type:
    SYS_LENGTH { $$ = SysFuncType::LENGTH; }
    | SYS_ROUND { $$ = SysFuncType::ROUND; }
    | SYS_DATE_FORMAT { $$ = SysFuncType::DATE_FORMAT; }
    ;

sys_func:
  sys_func_type LBRACE expression_list RBRACE
  {
    LOG_DEBUG("DEBUG: reduce sys_func");
    $$ = new SysFunctionExpr($1,*$3); // 拷贝构造
    $$->set_name(token_name(sql_string, &@$));
    context->add_object($$);
    delete $3; // 释放 expression_list
    context->remove_object($3);
  }
  ;

sub_query_expr:
    LBRACE select_stmt RBRACE {
      $$ = new SubqueryExpr($2);
      context->add_object($$);
      $$->set_name(token_name(sql_string, &@$));
    }
    ;

alias:
    /* empty */
    {
      $$ = nullptr;
    }
    | AS ID {
      $$ = $2;
    }
    | ID {
      $$ = $1;
    }
    ;

rel_attr:
    ID {
      $$ = new RelAttrSqlNode;
      context->add_object($$);
      $$->relation_name = string("");
      $$->attribute_name = $1;
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      context->add_object($$);
      $$->relation_name  = $1;
      $$->attribute_name = $3;
    }
    // 添加table.*功能,作为StarExpr
    | ID DOT '*' {
      $$ = new RelAttrSqlNode;
      context->add_object($$);
      $$->relation_name  = $1;
      $$->attribute_name = "*";
    }
    ;

relation:
    ID alias {
      $$ = new RelationSqlNode;
      context->add_object($$);
      $$->relation_name = $1;
      if ($2 != nullptr) {
        $$->alias_name = $2;
      }
    }
    ;
rel_list:
    relation {
      // $$ = new vector<string>();
      // $$->push_back($1);
      $$ = new vector<RelationSqlNode>;
      $$->emplace_back(*$1);
      context->add_object($$);
      context->remove_object($1);
      delete $1;
    }
    | relation COMMA rel_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        // $$ = new vector<string>;
        $$ = new vector<RelationSqlNode>;
        context->add_object($$);
      }
      $$->insert($$->begin(), *$1);
      // $$->insert($$->begin(), $1);
      context->remove_object($1);
      delete $1;
    }
    ;

where:
    /* empty */
    {
      $$ = nullptr;
    }
    | WHERE condition_list {
      $$ = $2;
    }
    ;
condition_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | condition {
      LOG_DEBUG("DEBUG: reduce condition");
      $$ = new vector<ConditionSqlNode>;
      context->add_object($$);
      $1->conjunction_type = ConjunctionType::NO_CONJUNCTION;
      $$->emplace_back(std::move(*$1));
      context->remove_object($1);
      delete $1;
    }
    | condition AND condition_list {
      $$ = $3;
      if ($$ == nullptr) {
        $$ = new vector<ConditionSqlNode>;
        context->add_object($$);
      }
      $1->conjunction_type = ConjunctionType::CONJ_AND;
      $$->insert($$->begin(), std::move(*$1));
      // $$->emplace_back(std::move(*$1));
      context->remove_object($1);
      delete $1;
    }
    ;
condition:
    expression comp_op expression
    {
      $$ = new ConditionSqlNode;
      context->add_object($$);
      context->remove_object($1);
      context->remove_object($3);
      $$->comp_op = $2;
      $$->left_expr = unique_ptr<Expression>($1);
      $$->right_expr = unique_ptr<Expression>($3);  
    }
    | exists_op expression
    {
      $$ = new ConditionSqlNode;
      context->add_object($$);
      context->remove_object($2);
      $$->comp_op = $1;
      // 后续需要判断是否为nullptr
      $$->left_expr = nullptr;
      $$->right_expr = unique_ptr<Expression>($2);
    }
    ;

comp_op:
      EQ { $$ = EQUAL_TO; }
    | LT { $$ = LESS_THAN; }
    | GT { $$ = GREAT_THAN; }
    | LE { $$ = LESS_EQUAL; }
    | GE { $$ = GREAT_EQUAL; }
    | NE { $$ = NOT_EQUAL; }
    | IS_TOKEN { $$ = CompOp::IS; }
    | IS_TOKEN NOT { $$ = IS_NOT; }
    | LIKE {$$ = LIKE_OP;}
    | NOT LIKE {$$ = NOT_LIKE_OP; }
    | IN { $$ = IN_OP; }
    | NOT IN { $$ = NOT_IN_OP; }
    ;

exists_op:
    EXISTS { $$ = EXISTS_OP; }
    | NOT EXISTS { $$ = NOT_EXISTS_OP; }
    ;
// your code here
group_by:
    /* empty */
    {
      $$ = nullptr;
    }
    ;
load_data_stmt:
    LOAD DATA INFILE SSS INTO TABLE ID 
    {
      char *tmp_file_name = common::substr($4, 1, strlen($4) - 2);
      $$ = new ParsedSqlNode(SCF_LOAD_DATA);
      context->add_object($$);
      $$->load_data.relation_name = $7;
      $$->load_data.file_name = tmp_file_name;
      free(tmp_file_name);
    }
    ;

explain_stmt:
    EXPLAIN command_wrapper
    {
      $$ = new ParsedSqlNode(SCF_EXPLAIN);
      context->add_object($$);
      $$->explain.sql_node = unique_ptr<ParsedSqlNode>($2);
    }
    ;

set_variable_stmt:
    SET ID EQ value
    {
      $$ = new ParsedSqlNode(SCF_SET_VARIABLE);
      context->add_object($$);
      $$->set_variable.name  = $2;
      $$->set_variable.value = *$4;
      context->remove_object($4);
      delete $4;
    }
    ;

opt_semicolon: /*empty*/
    | SEMICOLON
    ;
%%

extern void scan_string(const char *str, yyscan_t scanner);

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, 
           yyscan_t scanner, ParseContext *context, const char *msg) {
    unique_ptr<ParsedSqlNode> error_sql_node = make_unique<ParsedSqlNode>(SCF_ERROR);
    error_sql_node->error.error_msg = msg;
    error_sql_node->error.line = llocp->first_line;
    error_sql_node->error.column = llocp->first_column;
    sql_result->add_sql_node(std::move(error_sql_node));

    context->clear();
    return 0;
}

int sql_parse(const char *s, ParsedSqlResult *sql_result) {
    yyscan_t scanner;
    vector<char *> allocated_strings;
    ParseContext context;

    yylex_init_extra(static_cast<void*>(&allocated_strings), &scanner);
    scan_string(s, scanner);
    int result = yyparse(s, sql_result, scanner, &context);
    // context.clear();
    // automatically clean string
    for (char *ptr : allocated_strings) {
        free(ptr);
    }
    allocated_strings.clear();

    yylex_destroy(scanner);
    return result;
}