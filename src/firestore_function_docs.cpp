#include "firestore_function_docs.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "firestore_duckdb_compat.hpp"

namespace duckdb {

#ifdef FIRESTORE_DUCKDB_V2
// DuckDB 2.0 reads parameter names from the function's own signature, where a
// positional argument declared by type alone is named "col<N>". Name them.
static void NamePositionalArguments(TableFunctionSet &functions, const vector<FirestoreFunctionDoc> &docs) {
	for (idx_t i = 0; i < docs.size(); i++) {
		auto overload = make_shared_ptr<TableFunction>(*functions.functions[i]);
		auto &signature = overload->GetSignature();
		idx_t position = 0;
		for (idx_t p = 0; p < signature.GetParameterCount(); p++) {
			auto &parameter = signature.GetParameter(p);
			if (!parameter.AcceptsPosition() || parameter.IsVariadic()) {
				continue;
			}
			if (position >= docs[i].argument_names.size()) {
				break; // reported below, with the argument count
			}
			parameter.SetName(Identifier(docs[i].argument_names[position++]));
		}
		functions.functions[i] = std::move(overload);
	}
}
#endif

void RegisterDocumentedTableFunction(ExtensionLoader &loader, TableFunctionSet functions,
                                     const vector<FirestoreFunctionDoc> &docs) {
	const string name = FirestoreNameString(functions.name);
	if (functions.Size() != docs.size()) {
		throw InternalException("%s: %llu overloads but %llu docs", name, functions.Size(), docs.size());
	}
#ifdef FIRESTORE_DUCKDB_V2
	NamePositionalArguments(functions, docs);
#endif

	CreateTableFunctionInfo info(std::move(functions));
	// What the bare RegisterFunction(TableFunction) overload sets; CreateInfo
	// otherwise defaults to ERROR_ON_CONFLICT.
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

	for (idx_t i = 0; i < docs.size(); i++) {
		auto &doc = docs[i];
		// duckdb_functions() lists an overload's positional arguments, then its
		// named parameters in the order FirestoreNamedParameterNames returns them.
		// Moving info into the catalog moves the function list without touching its
		// elements.
		auto overload_entry = info.functions.GetFunctionByOffset(i);
#ifdef FIRESTORE_DUCKDB_V2
		const TableFunction &overload = *overload_entry;
#else
		const TableFunction &overload = overload_entry;
#endif
		auto arguments = FirestorePositionalArgumentTypes(overload);
		if (arguments.size() != doc.argument_names.size()) {
			throw InternalException("%s: overload %llu takes %llu arguments but names %llu", name, i, arguments.size(),
			                        doc.argument_names.size());
		}

		FunctionDescription description;
		// Matches the description to its overload when there are several.
		description.parameter_types = arguments;
		description.parameter_names = doc.argument_names;
		for (auto &param : FirestoreNamedParameterNames(overload)) {
			description.parameter_names.push_back(param);
		}
		description.description = doc.description;
		description.examples = {doc.example};
		description.categories = doc.categories;
		info.descriptions.push_back(std::move(description));
	}

	loader.RegisterFunction(std::move(info));
}

void RegisterDocumentedTableFunction(ExtensionLoader &loader, TableFunction function, const FirestoreFunctionDoc &doc) {
	TableFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterDocumentedTableFunction(loader, std::move(set), {doc});
}

} // namespace duckdb
