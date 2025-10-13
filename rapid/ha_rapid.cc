/* Copyright (c) 2018, 2025, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "ha_rapid.h"
#include "duckdb.hpp"

#include <stddef.h>
#include <algorithm>
#include <cassert>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>

#include "lex_string.h"
#include "my_alloc.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/debug_sync.h"
#include "sql/handler.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/make_join_hypergraph.h"
#include "sql/join_optimizer/walk_access_paths.h"
#include "sql/opt_trace.h"
#include "sql/sql_class.h"
#include "sql/sql_const.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "template_utils.h"
#include "thr_lock.h"

namespace dd {
class Table;
}

namespace {

struct RapidShare {
  THR_LOCK lock;
  RapidShare() { thr_lock_init(&lock); }
  ~RapidShare() { thr_lock_delete(&lock); }

  // Not copyable. The THR_LOCK object must stay where it is in memory
  // after it has been initialized.
  RapidShare(const RapidShare &) = delete;
  RapidShare &operator=(const RapidShare &) = delete;
};

// Map from (db_name, table_name) to the RapidShare with table state.
class LoadedTables {
  std::map<std::pair<std::string, std::string>, RapidShare> m_tables;
  std::mutex m_mutex;

 public:
  void add(const std::string &db, const std::string &table) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.emplace(std::piecewise_construct, std::make_tuple(db, table),
                     std::make_tuple());
  }

  RapidShare *get(const std::string &db, const std::string &table) {
    std::lock_guard<std::mutex> guard(m_mutex);
    auto it = m_tables.find(std::make_pair(db, table));
    return it == m_tables.end() ? nullptr : &it->second;
  }

  void erase(const std::string &db, const std::string &table) {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.erase(std::make_pair(db, table));
  }
};

LoadedTables *loaded_tables{nullptr};

/**
  Statement context class for the RAPID engine.
*/
class Rapid_statement_context : public Secondary_engine_statement_context {};

/**
  Execution context class for the RAPID engine. It allocates some data
  on the heap when it is constructed, and frees it when it is
  destructed, so that LeakSanitizer and Valgrind can detect if the
  server doesn't destroy the object when the query execution has
  completed.
*/
class Rapid_execution_context : public Secondary_engine_execution_context {
 public:
  Rapid_execution_context() : m_data(std::make_unique<char[]>(10)) {}
  /**
    Checks if the specified cost is the lowest cost seen so far for executing
    the given JOIN.
  */
  bool BestPlanSoFar(const JOIN &join, double cost) {
    if (&join != m_current_join) {
      // No plan has been seen for this join. The current one is best so far.
      m_current_join = &join;
      m_best_cost = cost;
      return true;
    }

    // Check if the current plan is the best seen so far.
    const bool cheaper = cost < m_best_cost;
    m_best_cost = std::min(m_best_cost, cost);
    return cheaper;
  }

 private:
  std::unique_ptr<char[]> m_data;
  /// The JOIN currently being optimized.
  const JOIN *m_current_join{nullptr};
  /// The cost of the best plan seen so far for the current JOIN.
  double m_best_cost;
};

}  // namespace

namespace rapid {

ha_rapid::ha_rapid(handlerton *hton, TABLE_SHARE *table_share_arg)
    : handler(hton, table_share_arg) {}

int ha_rapid::open(const char *, int, unsigned int, const dd::Table *) {
  RapidShare *share =
      loaded_tables->get(table_share->db.str, table_share->table_name.str);
  if (share == nullptr) {
    // The table has not been loaded into the secondary storage engine yet.
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "Table has not been loaded");
    return HA_ERR_GENERIC;
  }
  thr_lock_data_init(&share->lock, &m_lock, nullptr);
  return 0;
}

int ha_rapid::info(unsigned int flags) {
  // Get the cardinality statistics from the primary storage engine.
  handler *primary = ha_get_primary_handler();
  int ret = primary->info(flags);
  if (ret == 0) {
    stats.records = primary->stats.records;
  }
  return ret;
}

handler::Table_flags ha_rapid::table_flags() const {
  // Secondary engines do not support index access. Indexes are only used for
  // cost estimates.
  return HA_NO_INDEX_ACCESS;
}

unsigned long ha_rapid::index_flags(unsigned int idx, unsigned int part,
                                   bool all_parts) const {
  const handler *primary = ha_get_primary_handler();
  const unsigned long primary_flags =
      primary == nullptr ? 0 : primary->index_flags(idx, part, all_parts);

  // Inherit the following index flags from the primary handler, if they are
  // set:
  //
  // HA_READ_RANGE - to signal that ranges can be read from the index, so that
  // the optimizer can use the index to estimate the number of rows in a range.
  //
  // HA_KEY_SCAN_NOT_ROR - to signal if the index returns records in rowid
  // order. Used to disable use of the index in the range optimizer if it is not
  // in rowid order.
  return ((HA_READ_RANGE | HA_KEY_SCAN_NOT_ROR) & primary_flags);
}

ha_rows ha_rapid::records_in_range(unsigned int index, key_range *min_key,
                                  key_range *max_key) {
  // Get the number of records in the range from the primary storage engine.
  return ha_get_primary_handler()->records_in_range(index, min_key, max_key);
}

THR_LOCK_DATA **ha_rapid::store_lock(THD *, THR_LOCK_DATA **to,
                                    thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK)
    m_lock.type = lock_type;
  *to++ = &m_lock;
  return to;
}

std::string ha_rapid::escape_string(std::string in) {
  std::string out = "";
  for(int i=0;i<in.length();++i) {
    switch(in[i]) {
      /*case '\0':
        out += "\\0";
        break;*/
      case '\'':
        out += "\\'";
        break;
      default:
        out += in[i];
        break;  
    }
  }
  return out;
}

int ha_rapid::create_duckdb_table(const TABLE &table_arg, duckdb_connection con) {
  std::string create_table_query = "";

  for (Field **field = table_arg.field; *field; field++) {
    bool is_unsigned = (*field)->all_flags() & UNSIGNED_FLAG;
    bool is_nullable = (*field)->is_nullable();
    
    if(create_table_query != "") {
      create_table_query += ", ";
    }

    create_table_query += std::string((*field)->field_name) + " ";
    switch((*field)->real_type()) {
      case MYSQL_TYPE_TINY:
        create_table_query += "TINYINT";
        break;

      case MYSQL_TYPE_SHORT:
        create_table_query += "SMALLINT";
        break;

      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
        create_table_query += "INTEGER";
        break;

      case MYSQL_TYPE_LONGLONG:
        create_table_query += "BIGINT";
        break;

      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_STRING:
        create_table_query += "VARCHAR(" + std::to_string((*field)->field_length) + ")";
        break;

      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
        create_table_query += "BLOB";
        break;

      case MYSQL_TYPE_JSON:
        create_table_query += "JSON";
        case MYSQL_TYPE_FLOAT:
        break;

        create_table_query += "REAL";
        break;

      case MYSQL_TYPE_DOUBLE:
        create_table_query += "DOUBLE";
        break;

      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        create_table_query += "NUMERIC";
        break;

      case MYSQL_TYPE_YEAR:
        create_table_query += "INTEGER";
        break;

      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
        create_table_query += "DATE";
        break;

      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIME2:
        create_table_query += "TIME";
        break;

      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP2:
      case MYSQL_TYPE_DATETIME2:
        create_table_query += "TIMESTAMP";
        break;

      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_GEOMETRY:
      
      create_table_query += "VARCHAR";
        break;
    }
    
    if(is_unsigned) {
      create_table_query += " UNSIGNED";
    }
    
    if(!is_nullable) {
      create_table_query += " NOT NULL";
    }
    create_table_query += "\n";
    
  }
  std::string drop_table_query = "DROP TABLE IF EXISTS " +
                                 std::string(table_arg.s->db.str) + "_" +
                                 std::string(table_arg.s->table_name.str);

  create_table_query = "CREATE TABLE " +
                        std::string(table_arg.s->db.str) + "_" +
                        std::string(table_arg.s->table_name.str) +
                        "\n(\n" + create_table_query + "\n)\n";
                        
  if (duckdb_query(con, drop_table_query.c_str(), nullptr) == DuckDBError) {
    // handle error
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Could not drop table in DuckDB");
    return HA_ERR_GENERIC;
  }
  
  if (duckdb_query(con, create_table_query.c_str(), nullptr) == DuckDBError) {
    // handle error
    
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Could not create table in DuckDB");
    return HA_ERR_GENERIC;
  }

  return 0;
}

int ha_rapid::load_table(const TABLE &table_arg,
                        bool *skip_metadata_update [[maybe_unused]]) {
  assert(table_arg.file != nullptr);
  
  loaded_tables->add(table_arg.s->db.str, table_arg.s->table_name.str);
  
  if (loaded_tables->get(table_arg.s->db.str, table_arg.s->table_name.str) ==
      nullptr) {
    my_error(ER_NO_SUCH_TABLE, MYF(0), table_arg.s->db.str,
             table_arg.s->table_name.str);
    return HA_ERR_KEY_NOT_FOUND;
  }

  duckdb_connection con;

  if (duckdb_connect(db, &con) == DuckDBError) {
    // handle error
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Could not connect to DuckDB database");    
    return HA_ERR_GENERIC;
  }

  if(create_duckdb_table(table_arg, con)) {
    return HA_ERR_GENERIC;
  }

  auto res = table_arg.file->ha_rnd_init(true); 

  // Make the Field::ptr pointers valid by copying the row into record[0]
  while (table_arg.file->ha_rnd_next(table_arg.record[0]) == 0) {
    std::string insert_query = "";
                                
    for (Field **field = table_arg.field; *field; field++) {
            
      if (insert_query != "") {
        insert_query += ", ";
      }
      
      if ((*field)->is_null()) {
        insert_query += "NULL";
        continue;
      }
    
      String tmp;
      auto s = (*field)->val_str(&tmp);
      if((*field)->str_needs_quotes()) {
        insert_query += "e'"+ escape_string(std::string(s->ptr(), s->length())) + "'";
      } else {
        insert_query += std::string(s->ptr(), s->length());
      }
      
    }
    insert_query = "INSERT INTO " +
                    std::string(table_arg.s->db.str) + "_" +
                    std::string(table_arg.s->table_name.str) +
                    " VALUES (" + insert_query + ")";
    
    if (duckdb_query(con, insert_query.c_str(), nullptr) == DuckDBError) {
      // handle error
      my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
                "Could not insert data into table in DuckDB");
    }     
  }
  
  table_arg.file->ha_rnd_end();
  // cleanup
  duckdb_disconnect(&con);
  
  return 0;
}

int ha_rapid::unload_table(const char *db_name, const char *table_name,
                          bool error_if_not_loaded) {
  if (error_if_not_loaded &&
      loaded_tables->get(db_name, table_name) == nullptr) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Table is not loaded on a secondary engine");
    return 1;
  } else {
    
    duckdb_connection con;

    if (duckdb_connect(db, &con) == DuckDBError) {
      // handle error
      my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
               "Could not connect to DuckDB database");     
    }
    std::string drop_table_query = "DROP TABLE IF EXISTS " + std::string(db_name) + "_" + std::string(table_name);
    if (duckdb_query(con, drop_table_query.c_str(), nullptr) == DuckDBError) {
      // handle error
      my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
               "Could not drop table in DuckDB");
    }
    // cleanup
    duckdb_disconnect(&con);
    
  }
  loaded_tables->erase(db_name, table_name);
  my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
               "Dropped table in DuckDB");
  return 0;
}

}  // namespace rapid

namespace {
bool SecondaryEnginePrePrepareHook(THD *thd) {
  if (thd->m_current_query_cost <=
      static_cast<double>(thd->variables.secondary_engine_cost_threshold)) {
    Opt_trace_context *const trace = &thd->opt_trace;
    if (trace->is_started()) {
      const Opt_trace_object wrapper(trace);
      Opt_trace_object oto(trace, "secondary_engine_not_used");
      oto.add_alnum("reason",
                    "The estimated query cost does not exceed "
                    "secondary_engine_cost_threshold.");
      oto.add("cost", thd->m_current_query_cost);
      oto.add("threshold", thd->variables.secondary_engine_cost_threshold);
    }
    return false;
  }

  if (thd->secondary_engine_statement_context() == nullptr) {
    /* Prepare this query's specific statment context */
    std::unique_ptr<Secondary_engine_statement_context> ctx =
        std::make_unique<Rapid_statement_context>();
    thd->set_secondary_engine_statement_context(std::move(ctx));
  }
  return true;
}
}  // namespace

static bool PrepareSecondaryEngine(THD *thd, LEX *lex) {
  DBUG_EXECUTE_IF("secondary_engine_rapid_prepare_error", {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "");
    return true;
  });

  auto context = new (thd->mem_root) Rapid_execution_context;
  if (context == nullptr) return true;
  lex->set_secondary_engine_execution_context(context);

  // Disable use of constant tables and evaluation of subqueries during
  // optimization.
  lex->add_statement_options(OPTION_NO_CONST_TABLES |
                             OPTION_NO_SUBQUERY_DURING_OPTIMIZATION);

  return false;
}

static void AssertSupportedPath(const AccessPath *path) {
  switch (path->type) {
    // The only supported join type is hash join. Other join types are disabled
    // in handlerton::secondary_engine_flags.
    case AccessPath::NESTED_LOOP_JOIN: /* purecov: deadcode */
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
    case AccessPath::BKA_JOIN:
    // Index access is disabled in ha_rapid::table_flags(), so we should see none
    // of these access types.
    case AccessPath::INDEX_SCAN:
    case AccessPath::INDEX_DISTANCE_SCAN: /* purecov: deadcode */
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::EQ_REF:
    case AccessPath::PUSHED_JOIN_REF:
    case AccessPath::INDEX_RANGE_SCAN:
    case AccessPath::INDEX_SKIP_SCAN:
    case AccessPath::GROUP_INDEX_SKIP_SCAN:
    case AccessPath::ROWID_INTERSECTION:
    case AccessPath::ROWID_UNION:
    case AccessPath::DYNAMIC_INDEX_RANGE_SCAN:
      assert(false); /* purecov: deadcode */
      break;
    default:
      break;
  }

  // This secondary storage engine does not yet store anything in the auxiliary
  // data member of AccessPath.
  assert(path->secondary_engine_data == nullptr);
}

static bool OptimizeSecondaryEngine(THD *thd [[maybe_unused]], LEX *lex) {
  // The context should have been set by PrepareSecondaryEngine.
  assert(lex->secondary_engine_execution_context() != nullptr);

  DBUG_EXECUTE_IF("secondary_engine_rapid_optimize_error", {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "");
    return true;
  });

  DEBUG_SYNC(thd, "before_rapid_optimize");

  if (lex->using_hypergraph_optimizer()) {
    WalkAccessPaths(lex->unit->root_access_path(), nullptr,
                    WalkAccessPathPolicy::ENTIRE_TREE,
                    [](AccessPath *path, const JOIN *) {
                      AssertSupportedPath(path);
                      return false;
                    });
  }

  return false;
}

static bool CompareJoinCost(THD *thd, const JOIN &join, double optimizer_cost,
                            bool *use_best_so_far, bool *cheaper,
                            double *secondary_engine_cost) {
  *use_best_so_far = false;

  DBUG_EXECUTE_IF("secondary_engine_rapid_compare_cost_error", {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "");
    return true;
  });

  DBUG_EXECUTE_IF("secondary_engine_rapid_choose_first_plan", {
    *use_best_so_far = true;
    *cheaper = true;
    *secondary_engine_cost = optimizer_cost;
  });

  // Just use the cost calculated by the optimizer by default.
  *secondary_engine_cost = optimizer_cost;

  // This debug flag makes the cost function prefer orders where a table with
  // the alias "X" is closer to the beginning.
  DBUG_EXECUTE_IF("secondary_engine_rapid_change_join_order", {
    double cost = join.tables;
    for (size_t i = 0; i < join.tables; ++i) {
      const Table_ref *ref = join.positions[i].table->table_ref;
      if (std::string(ref->alias) == "X") {
        cost += i;
      }
    }
    *secondary_engine_cost = cost;
  });

  // Check if the calculated cost is cheaper than the best cost seen so far.
  *cheaper = down_cast<Rapid_execution_context *>(
                 thd->lex->secondary_engine_execution_context())
                 ->BestPlanSoFar(join, *secondary_engine_cost);

  return false;
}

static bool ModifyAccessPathCost(THD *thd [[maybe_unused]],
                                 const JoinHypergraph &hypergraph
                                 [[maybe_unused]],
                                 AccessPath *path) {
  assert(!thd->is_error());
  assert(hypergraph.query_block()->join == hypergraph.join());
  AssertSupportedPath(path);
  return false;
}

static handler *Create(handlerton *hton, TABLE_SHARE *table_share, bool,
                       MEM_ROOT *mem_root) {
  return new (mem_root) rapid::ha_rapid(hton, table_share);
}

static int Init(MYSQL_PLUGIN p) {
  loaded_tables = new LoadedTables();

  handlerton *hton = static_cast<handlerton *>(p);
  hton->create = Create;
  hton->state = SHOW_OPTION_YES;
  hton->flags = HTON_IS_SECONDARY_ENGINE;
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->prepare_secondary_engine = PrepareSecondaryEngine;
  hton->secondary_engine_pre_prepare_hook = SecondaryEnginePrePrepareHook;
  hton->optimize_secondary_engine = OptimizeSecondaryEngine;
  hton->compare_secondary_engine_cost = CompareJoinCost;
  hton->secondary_engine_flags =
      MakeSecondaryEngineFlags(SecondaryEngineFlag::SUPPORTS_HASH_JOIN);
  hton->secondary_engine_modify_access_path_cost = ModifyAccessPathCost;

  if (duckdb_open(":memory:", &db) == DuckDBError) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Could not open DuckDB database");
  }  
  return 0;
}

static int Deinit(MYSQL_PLUGIN) {
  delete loaded_tables;
  loaded_tables = nullptr;
  duckdb_close(&db);
  return 0;
}

static st_mysql_storage_engine rapid_storage_engine{
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(rapid){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &rapid_storage_engine,
    "RAPID",
    PLUGIN_AUTHOR_ORACLE,
    "Rapid storage engine",
    PLUGIN_LICENSE_GPL,
    Init,
    nullptr,
    Deinit,
    0x0001,
    nullptr,
    nullptr,
    nullptr,
    0,
} mysql_declare_plugin_end;
