#include "firestore_function_docs.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

void RegisterDocumentedTableFunction(ExtensionLoader &loader, TableFunctionSet functions,
                                     const vector<FirestoreFunctionDoc> &docs) {
	const auto name = functions.name;
	if (functions.Size() != docs.size()) {
		throw InternalException("%s: %llu overloads but %llu docs", name, functions.Size(), docs.size());
	}

	CreateTableFunctionInfo info(std::move(functions));
	// What the bare RegisterFunction(TableFunction) overload sets; CreateInfo
	// otherwise defaults to ERROR_ON_CONFLICT.
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;

	for (idx_t i = 0; i < docs.size(); i++) {
		auto &doc = docs[i];
		// duckdb_functions() lists an overload's positional arguments, then its named
		// parameters in the order it iterates a copy taken by GetFunctionByOffset. That
		// map is unordered and its order differs between standard libraries, so the
		// named parameters are read back the same way rather than written out here.
		// Moving info into the catalog moves this vector without touching its elements.
		auto overload = info.functions.GetFunctionByOffset(i);
		if (overload.arguments.size() != doc.argument_names.size()) {
			throw InternalException("%s: overload %llu takes %llu arguments but names %llu", name, i,
			                        overload.arguments.size(), doc.argument_names.size());
		}

		FunctionDescription description;
		// Matches the description to its overload when there are several.
		description.parameter_types = overload.arguments;
		description.parameter_names = doc.argument_names;
		for (auto &param : overload.named_parameters) {
			description.parameter_names.push_back(param.first);
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
