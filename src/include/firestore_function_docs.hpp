#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

// Documentation for one table-function overload, as duckdb_functions() reports it.
// That catalog view is the only documentation a SQL connection can reach, so it is
// how tools and agents discover what each function does and how to call it.
struct FirestoreFunctionDoc {
	// Names of the positional arguments, in order. Named parameters are listed after
	// them automatically, under their own names.
	vector<string> argument_names;
	// What the function does.
	string description;
	// One runnable statement; table functions are not valid as bare expressions.
	string example;
	// Short tags for grouping, such as {"firestore", "write"}.
	vector<string> categories;
};

// Register a table function whose overloads are documented by docs, in the same order.
void RegisterDocumentedTableFunction(ExtensionLoader &loader, TableFunctionSet functions,
                                     const vector<FirestoreFunctionDoc> &docs);

// Register a single-overload table function documented by doc.
void RegisterDocumentedTableFunction(ExtensionLoader &loader, TableFunction function, const FirestoreFunctionDoc &doc);

} // namespace duckdb
