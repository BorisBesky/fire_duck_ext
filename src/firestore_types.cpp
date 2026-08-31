#include "firestore_types.hpp"
#include "firestore_logger.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/string_type.hpp"
#include <map>
#include <set>

namespace duckdb {

// Base64 encode/decode for bytesValue. DuckDB's Blob helpers produce and consume
// standard RFC 4648 base64 (with padding) — the format the Firestore REST API uses —
// and are available on every platform, including WASM where OpenSSL is not linked.
static std::string Base64Encode(const std::string &data) {
	return Blob::ToBase64(string_t(data.data(), static_cast<uint32_t>(data.size())));
}

// Base64 decoding for bytesValue
static std::string Base64Decode(const std::string &encoded) {
	try {
		return Blob::FromBase64(string_t(encoded.data(), static_cast<uint32_t>(encoded.size())));
	} catch (...) {
		return encoded; // Return as-is if decoding fails, preserving prior behavior
	}
}

FirestoreMapEncoding ParseMapEncoding(const std::string &name) {
	if (name == "wire") {
		return FirestoreMapEncoding::WIRE;
	}
	if (name == "json") {
		return FirestoreMapEncoding::JSON;
	}
	if (name == "variant") {
		return FirestoreMapEncoding::VARIANT;
	}
	throw InvalidInputException("Unknown map_encoding '%s'. Expected one of: wire, json, variant.", name);
}

const char *MapEncodingName(FirestoreMapEncoding encoding) {
	switch (encoding) {
	case FirestoreMapEncoding::JSON:
		return "json";
	case FirestoreMapEncoding::VARIANT:
		return "variant";
	default:
		return "wire";
	}
}

// Firestore encodes 64-bit integers as JSON strings to survive round-tripping
// through JSON number precision. Recover the integer where possible.
static bool TryParseFirestoreInteger(const json &val, int64_t &out) {
	if (val.is_string()) {
		try {
			out = std::stoll(val.get<std::string>());
			return true;
		} catch (const std::exception &) {
			return false;
		}
	}
	if (val.is_number_integer()) {
		out = val.get<int64_t>();
		return true;
	}
	return false;
}

json UnwrapFirestoreValue(const json &fv) {
	if (fv.contains("nullValue")) {
		return nullptr;
	}
	if (fv.contains("stringValue")) {
		return fv["stringValue"];
	}
	if (fv.contains("integerValue")) {
		int64_t parsed;
		if (TryParseFirestoreInteger(fv["integerValue"], parsed)) {
			return parsed;
		}
		return fv["integerValue"]; // unparseable: keep the raw form rather than lose it
	}
	if (fv.contains("doubleValue")) {
		return fv["doubleValue"];
	}
	if (fv.contains("booleanValue")) {
		return fv["booleanValue"];
	}
	// Timestamps, references and bytes have no native JSON counterpart; keep
	// their Firestore textual form (ISO-8601, document path, base64).
	if (fv.contains("timestampValue")) {
		return fv["timestampValue"];
	}
	if (fv.contains("referenceValue")) {
		return fv["referenceValue"];
	}
	if (fv.contains("bytesValue")) {
		return fv["bytesValue"];
	}
	if (fv.contains("geoPointValue")) {
		const auto &geo = fv["geoPointValue"];
		json out = json::object();
		out["latitude"] = geo.contains("latitude") ? geo["latitude"].get<double>() : 0.0;
		out["longitude"] = geo.contains("longitude") ? geo["longitude"].get<double>() : 0.0;
		return out;
	}
	if (fv.contains("arrayValue")) {
		json out = json::array();
		if (fv["arrayValue"].contains("values")) {
			for (const auto &elem : fv["arrayValue"]["values"]) {
				out.push_back(UnwrapFirestoreValue(elem));
			}
		}
		return out;
	}
	if (fv.contains("mapValue")) {
		json out = json::object();
		if (fv["mapValue"].contains("fields")) {
			const auto &fields = fv["mapValue"]["fields"];
			for (auto it = fields.begin(); it != fields.end(); ++it) {
				out[it.key()] = UnwrapFirestoreValue(it.value());
			}
		}
		return out;
	}
	return fv; // unrecognised: pass through untouched
}

VariantValue FirestoreValueToVariant(const json &fv) {
	if (fv.contains("nullValue")) {
		return VariantValue::NullValue();
	}
	if (fv.contains("stringValue")) {
		return VariantValue(Value(fv["stringValue"].get<std::string>()));
	}
	if (fv.contains("integerValue")) {
		int64_t parsed;
		if (TryParseFirestoreInteger(fv["integerValue"], parsed)) {
			return VariantValue(Value::BIGINT(parsed));
		}
		return VariantValue(Value(fv["integerValue"].dump()));
	}
	if (fv.contains("doubleValue")) {
		return VariantValue(Value::DOUBLE(fv["doubleValue"].get<double>()));
	}
	if (fv.contains("booleanValue")) {
		return VariantValue(Value::BOOLEAN(fv["booleanValue"].get<bool>()));
	}
	if (fv.contains("timestampValue")) {
		// Keep the parsed timestamp so variant_typeof reports TIMESTAMP rather
		// than VARCHAR; fall back to the raw text when it will not parse.
		std::string ts_str = fv["timestampValue"].get<std::string>();
		std::string normalised = ts_str;
		if (!normalised.empty() && normalised.back() == 'Z') {
			normalised.pop_back();
		}
		size_t t_pos = normalised.find('T');
		if (t_pos != std::string::npos) {
			normalised[t_pos] = ' ';
		}
		try {
			return VariantValue(Value::TIMESTAMP(Timestamp::FromString(normalised, false)));
		} catch (const std::exception &) {
			return VariantValue(Value(ts_str));
		}
	}
	if (fv.contains("referenceValue")) {
		return VariantValue(Value(fv["referenceValue"].get<std::string>()));
	}
	if (fv.contains("bytesValue")) {
		// BLOB_RAW takes the decoded octets as they are. Value::BLOB(string)
		// would cast instead, reading \x escapes and rejecting any byte above
		// 0x7f -- which failed the whole query on ordinary binary data.
		return VariantValue(Value::BLOB_RAW(Base64Decode(fv["bytesValue"].get<std::string>())));
	}
	if (fv.contains("geoPointValue")) {
		const auto &geo = fv["geoPointValue"];
		VariantValue obj(VariantValueType::OBJECT);
		obj.AddChild("latitude",
		             VariantValue(Value::DOUBLE(geo.contains("latitude") ? geo["latitude"].get<double>() : 0.0)));
		obj.AddChild("longitude",
		             VariantValue(Value::DOUBLE(geo.contains("longitude") ? geo["longitude"].get<double>() : 0.0)));
		return obj;
	}
	if (fv.contains("arrayValue")) {
		VariantValue arr(VariantValueType::ARRAY);
		if (fv["arrayValue"].contains("values")) {
			const auto &values = fv["arrayValue"]["values"];
			arr.ReserveItems(values.size());
			for (const auto &elem : values) {
				arr.AddItem(FirestoreValueToVariant(elem));
			}
		}
		return arr;
	}
	if (fv.contains("mapValue")) {
		VariantValue obj(VariantValueType::OBJECT);
		if (fv["mapValue"].contains("fields")) {
			const auto &fields = fv["mapValue"]["fields"];
			for (auto it = fields.begin(); it != fields.end(); ++it) {
				obj.AddChild(it.key(), FirestoreValueToVariant(it.value()));
			}
		}
		return obj;
	}
	return VariantValue(Value(fv.dump()));
}

LogicalType FirestoreTypeToDuckDB(const std::string &firestore_type, FirestoreMapEncoding map_encoding) {
	if (firestore_type == "stringValue")
		return LogicalType::VARCHAR;
	if (firestore_type == "integerValue")
		return LogicalType::BIGINT;
	if (firestore_type == "doubleValue")
		return LogicalType::DOUBLE;
	if (firestore_type == "booleanValue")
		return LogicalType::BOOLEAN;
	if (firestore_type == "timestampValue")
		return LogicalType::TIMESTAMP;
	if (firestore_type == "referenceValue")
		return LogicalType::VARCHAR;
	if (firestore_type == "bytesValue")
		return LogicalType::BLOB;
	if (firestore_type == "nullValue")
		return LogicalType::VARCHAR; // Will be null

	if (firestore_type == "vectorValue") {
		// Vectors become ARRAY(DOUBLE, N) - dimension N is determined during schema inference
		// Fallback to LIST(DOUBLE) when dimension is unknown
		return LogicalType::LIST(LogicalType::DOUBLE);
	}

	if (firestore_type == "geoPointValue") {
		// GeoPoints become STRUCT(latitude DOUBLE, longitude DOUBLE)
		child_list_t<LogicalType> struct_children;
		struct_children.push_back(make_pair("latitude", LogicalType::DOUBLE));
		struct_children.push_back(make_pair("longitude", LogicalType::DOUBLE));
		return LogicalType::STRUCT(struct_children);
	}

	if (firestore_type == "arrayValue") {
		// Arrays become LIST(VARCHAR) by default
		// Element type may be refined during schema inference
		return LogicalType::LIST(LogicalType::VARCHAR);
	}

	if (firestore_type == "mapValue") {
		switch (map_encoding) {
		case FirestoreMapEncoding::VARIANT:
			return LogicalType::VARIANT();
		case FirestoreMapEncoding::JSON:
			return LogicalType::JSON();
		default:
			return LogicalType::VARCHAR; // raw Firestore wire JSON
		}
	}

	// Default fallback
	FS_LOG_DEBUG("Unknown Firestore type: " + firestore_type + ", defaulting to VARCHAR");
	return LogicalType::VARCHAR;
}

LogicalType InferDuckDBType(const json &firestore_value) {
	std::string type_name = GetFirestoreTypeName(firestore_value);
	return FirestoreTypeToDuckDB(type_name);
}

Value FirestoreValueToDuckDB(const json &fv, const LogicalType &target_type, FirestoreMapEncoding map_encoding) {
	if (IsFirestoreNull(fv)) {
		return Value(target_type); // NULL value with proper type
	}

	if (fv.contains("stringValue")) {
		return Value(fv["stringValue"].get<std::string>());
	}

	if (fv.contains("integerValue")) {
		// Firestore sends integers as strings in JSON
		auto val = fv["integerValue"];
		if (val.is_string()) {
			try {
				return Value::BIGINT(std::stoll(val.get<std::string>()));
			} catch (const std::exception &e) {
				FS_LOG_WARN("Failed to parse integerValue '" + val.get<std::string>() + "': " + e.what());
				return Value(target_type); // Return NULL
			}
		}
		return Value::BIGINT(val.get<int64_t>());
	}

	if (fv.contains("doubleValue")) {
		return Value::DOUBLE(fv["doubleValue"].get<double>());
	}

	if (fv.contains("booleanValue")) {
		return Value::BOOLEAN(fv["booleanValue"].get<bool>());
	}

	if (fv.contains("timestampValue")) {
		std::string ts_str = fv["timestampValue"].get<std::string>();
		// Parse ISO 8601 timestamp (e.g., "2024-01-15T10:30:00.000Z")
		// Remove trailing Z and parse
		if (!ts_str.empty() && ts_str.back() == 'Z') {
			ts_str.pop_back();
		}
		// Replace T with space for DuckDB parsing
		size_t t_pos = ts_str.find('T');
		if (t_pos != std::string::npos) {
			ts_str[t_pos] = ' ';
		}
		try {
			return Value::TIMESTAMP(Timestamp::FromString(ts_str, false));
		} catch (const std::exception &e) {
			// Fallback to string if parsing fails
			FS_LOG_WARN("Failed to parse timestamp '" + fv["timestampValue"].get<std::string>() + "': " + e.what() +
			            ", returning as string");
			return Value(fv["timestampValue"].get<std::string>());
		}
	}

	if (fv.contains("geoPointValue")) {
		// Convert geopoint to STRUCT(latitude DOUBLE, longitude DOUBLE)
		auto &geo = fv["geoPointValue"];
		double lat = geo.contains("latitude") ? geo["latitude"].get<double>() : 0.0;
		double lng = geo.contains("longitude") ? geo["longitude"].get<double>() : 0.0;

		child_list_t<Value> struct_values;
		struct_values.push_back(make_pair("latitude", Value::DOUBLE(lat)));
		struct_values.push_back(make_pair("longitude", Value::DOUBLE(lng)));
		return Value::STRUCT(struct_values);
	}

	if (fv.contains("arrayValue")) {
		// Convert to native DuckDB LIST type
		vector<Value> list_values;

		// Determine the element type from target_type if it's a LIST
		LogicalType element_type = LogicalType::VARCHAR;
		if (target_type.id() == LogicalTypeId::LIST) {
			element_type = ListType::GetChildType(target_type);
		}

		if (fv["arrayValue"].contains("values")) {
			for (auto &elem : fv["arrayValue"]["values"]) {
				// Convert each element to DuckDB Value
				if (elem.contains("nullValue")) {
					list_values.push_back(Value(element_type)); // NULL with proper type
				} else if (elem.contains("stringValue")) {
					if (element_type.id() == LogicalTypeId::VARCHAR) {
						list_values.push_back(Value(elem["stringValue"].get<std::string>()));
					} else {
						// Type mismatch - convert to string representation
						list_values.push_back(Value(elem["stringValue"].get<std::string>()));
					}
				} else if (elem.contains("integerValue")) {
					auto val = elem["integerValue"];
					int64_t int_val;
					if (val.is_string()) {
						try {
							int_val = std::stoll(val.get<std::string>());
						} catch (const std::exception &e) {
							FS_LOG_DEBUG("Array element integer parse failed: " + std::string(e.what()));
							list_values.push_back(Value(element_type)); // NULL on parse failure
							continue;
						}
					} else {
						int_val = val.get<int64_t>();
					}
					if (element_type.id() == LogicalTypeId::BIGINT) {
						list_values.push_back(Value::BIGINT(int_val));
					} else if (element_type.id() == LogicalTypeId::DOUBLE) {
						list_values.push_back(Value::DOUBLE(static_cast<double>(int_val)));
					} else {
						list_values.push_back(Value(std::to_string(int_val)));
					}
				} else if (elem.contains("doubleValue")) {
					double dbl_val = elem["doubleValue"].get<double>();
					if (element_type.id() == LogicalTypeId::DOUBLE) {
						list_values.push_back(Value::DOUBLE(dbl_val));
					} else if (element_type.id() == LogicalTypeId::BIGINT) {
						list_values.push_back(Value::BIGINT(static_cast<int64_t>(dbl_val)));
					} else {
						list_values.push_back(Value(std::to_string(dbl_val)));
					}
				} else if (elem.contains("booleanValue")) {
					bool bool_val = elem["booleanValue"].get<bool>();
					if (element_type.id() == LogicalTypeId::BOOLEAN) {
						list_values.push_back(Value::BOOLEAN(bool_val));
					} else {
						list_values.push_back(Value(bool_val ? "true" : "false"));
					}
				} else if (elem.contains("mapValue")) {
					// Maps become JSON strings within the array
					if (elem["mapValue"].contains("fields")) {
						list_values.push_back(Value(elem["mapValue"]["fields"].dump()));
					} else {
						list_values.push_back(Value("{}"));
					}
				} else if (elem.contains("arrayValue")) {
					// Nested arrays - recursively convert to LIST
					Value nested = FirestoreValueToDuckDB(elem, LogicalType::LIST(element_type), map_encoding);
					// For nested arrays in a VARCHAR list, serialize to JSON
					if (element_type.id() == LogicalTypeId::VARCHAR) {
						// Convert list to JSON array string
						json arr = json::array();
						if (nested.type().id() == LogicalTypeId::LIST) {
							auto &children = ListValue::GetChildren(nested);
							for (auto &child : children) {
								if (child.IsNull()) {
									arr.push_back(nullptr);
								} else {
									arr.push_back(child.ToString());
								}
							}
						}
						list_values.push_back(Value(arr.dump()));
					} else {
						list_values.push_back(nested);
					}
				} else {
					// Unknown type - convert to string
					list_values.push_back(Value(elem.dump()));
				}
			}
		}

		return Value::LIST(element_type, list_values);
	}

	// Check for vector type (special mapValue with __type__: __vector__)
	if (IsFirestoreVector(fv)) {
		vector<Value> double_values;
		const auto &arr = fv["mapValue"]["fields"]["value"]["arrayValue"];
		if (arr.contains("values")) {
			for (const auto &elem : arr["values"]) {
				if (elem.contains("doubleValue")) {
					double_values.push_back(Value::DOUBLE(elem["doubleValue"].get<double>()));
				} else if (elem.contains("integerValue")) {
					auto val = elem["integerValue"];
					double dval;
					if (val.is_string()) {
						dval = std::stod(val.get<std::string>());
					} else {
						dval = static_cast<double>(val.get<int64_t>());
					}
					double_values.push_back(Value::DOUBLE(dval));
				} else if (elem.contains("nullValue")) {
					double_values.push_back(Value(LogicalType::DOUBLE));
				} else {
					FS_LOG_DEBUG("Unexpected element type in vector value: " + elem.dump());
					double_values.push_back(Value(LogicalType::DOUBLE));
				}
			}
		}

		// Return as ARRAY if target type is ARRAY, otherwise as LIST
		if (target_type.id() == LogicalTypeId::ARRAY) {
			return Value::ARRAY(LogicalType::DOUBLE, double_values);
		}
		return Value::LIST(LogicalType::DOUBLE, double_values);
	}

	if (fv.contains("mapValue")) {
		if (map_encoding == FirestoreMapEncoding::JSON) {
			return Value(UnwrapFirestoreValue(fv).dump());
		}
		// WIRE: the Firestore payload verbatim, type wrappers and all.
		// (VARIANT never reaches here -- the scanner builds those per chunk.)
		if (fv["mapValue"].contains("fields")) {
			return Value(fv["mapValue"]["fields"].dump());
		}
		return Value("{}");
	}

	if (fv.contains("referenceValue")) {
		return Value(fv["referenceValue"].get<std::string>());
	}

	if (fv.contains("bytesValue")) {
		std::string b64 = fv["bytesValue"].get<std::string>();
		// Decode base64 to binary data
		std::string decoded = Base64Decode(b64);
		// The octets are already binary; casting them as a string would reject
		// every byte above 0x7f.
		return Value::BLOB_RAW(decoded);
	}

	// Unknown type - return as string
	FS_LOG_DEBUG("Unknown Firestore value type, converting to string: " + fv.dump().substr(0, 100));
	return Value(fv.dump());
}

json DuckDBValueToFirestore(const Value &value, const LogicalType &source_type) {
	if (value.IsNull()) {
		return {{"nullValue", nullptr}};
	}

	switch (source_type.id()) {
	case LogicalTypeId::VARCHAR:
		return {{"stringValue", value.GetValue<std::string>()}};

	case LogicalTypeId::BIGINT:
		return {{"integerValue", std::to_string(value.GetValue<int64_t>())}};

	case LogicalTypeId::INTEGER:
		return {{"integerValue", std::to_string(value.GetValue<int32_t>())}};

	case LogicalTypeId::SMALLINT:
		return {{"integerValue", std::to_string(value.GetValue<int16_t>())}};

	case LogicalTypeId::TINYINT:
		return {{"integerValue", std::to_string(value.GetValue<int8_t>())}};

	case LogicalTypeId::HUGEINT:
		return {{"integerValue", value.GetValue<hugeint_t>().ToString()}};

	case LogicalTypeId::UBIGINT:
		return {{"integerValue", std::to_string(value.GetValue<uint64_t>())}};

	case LogicalTypeId::UINTEGER:
		return {{"integerValue", std::to_string(value.GetValue<uint32_t>())}};

	case LogicalTypeId::USMALLINT:
		return {{"integerValue", std::to_string(value.GetValue<uint16_t>())}};

	case LogicalTypeId::UTINYINT:
		return {{"integerValue", std::to_string(value.GetValue<uint8_t>())}};

	case LogicalTypeId::DOUBLE:
		return {{"doubleValue", value.GetValue<double>()}};

	case LogicalTypeId::FLOAT:
		return {{"doubleValue", static_cast<double>(value.GetValue<float>())}};

	case LogicalTypeId::DECIMAL:
		// DECIMAL types (e.g., 3.14 literal inferred as DECIMAL(3,2))
		return {{"doubleValue", value.GetValue<double>()}};

	case LogicalTypeId::BOOLEAN:
		return {{"booleanValue", value.GetValue<bool>()}};

	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto ts = value.GetValue<timestamp_t>();
		std::string ts_str = Timestamp::ToString(ts);
		// Convert to ISO 8601 format
		size_t space_pos = ts_str.find(' ');
		if (space_pos != std::string::npos) {
			ts_str[space_pos] = 'T';
		}
		ts_str += "Z";
		return {{"timestampValue", ts_str}};
	}

	case LogicalTypeId::BLOB: {
		// Base64 encode the binary data for Firestore. As on the read side,
		// StringValue::Get reads the octets themselves; casting to VARCHAR
		// first would encode the \xNN escape text rather than the bytes.
		const auto &blob_data = StringValue::Get(value);
		return {{"bytesValue", Base64Encode(blob_data)}};
	}

	case LogicalTypeId::LIST: {
		json arr_values = json::array();
		auto &list = ListValue::GetChildren(value);
		auto element_type = ListType::GetChildType(source_type);
		for (auto &elem : list) {
			arr_values.push_back(DuckDBValueToFirestore(elem, element_type));
		}
		return {{"arrayValue", {{"values", arr_values}}}};
	}

	case LogicalTypeId::ARRAY: {
		// ARRAY(DOUBLE, N) -> Firestore vector format (mapValue with __type__: __vector__)
		json arr_values = json::array();
		auto &children = ArrayValue::GetChildren(value);
		for (auto &elem : children) {
			if (elem.IsNull()) {
				arr_values.push_back({{"doubleValue", 0.0}});
			} else {
				arr_values.push_back({{"doubleValue", elem.GetValue<double>()}});
			}
		}
		return {{"mapValue",
		         {{"fields",
		           {{"__type__", {{"stringValue", "__vector__"}}},
		            {"value", {{"arrayValue", {{"values", arr_values}}}}}}}}}};
	}

	case LogicalTypeId::STRUCT: {
		// Check if this is a geopoint struct (has latitude and longitude)
		auto &child_types = StructType::GetChildTypes(source_type);
		bool is_geopoint = false;
		if (child_types.size() == 2) {
			bool has_lat = false, has_lng = false;
			for (const auto &child : child_types) {
				if (child.first == "latitude" && child.second.id() == LogicalTypeId::DOUBLE)
					has_lat = true;
				if (child.first == "longitude" && child.second.id() == LogicalTypeId::DOUBLE)
					has_lng = true;
			}
			is_geopoint = has_lat && has_lng;
		}

		auto &children = StructValue::GetChildren(value);

		if (is_geopoint) {
			// Convert to geoPointValue
			double lat = 0.0, lng = 0.0;
			for (idx_t i = 0; i < children.size(); i++) {
				if (child_types[i].first == "latitude") {
					lat = children[i].IsNull() ? 0.0 : children[i].GetValue<double>();
				} else if (child_types[i].first == "longitude") {
					lng = children[i].IsNull() ? 0.0 : children[i].GetValue<double>();
				}
			}
			return {{"geoPointValue", {{"latitude", lat}, {"longitude", lng}}}};
		}

		// Regular struct -> mapValue
		json fields;
		for (idx_t i = 0; i < children.size(); i++) {
			fields[child_types[i].first] = DuckDBValueToFirestore(children[i], child_types[i].second);
		}
		return {{"mapValue", {{"fields", fields}}}};
	}

	default:
		// Fallback to string representation
		FS_LOG_DEBUG("Unknown DuckDB type for Firestore conversion: " + source_type.ToString() + ", using string");
		return {{"stringValue", value.ToString()}};
	}
}

void SetDuckDBValue(Vector &vector, idx_t index, const json &firestore_value, const LogicalType &type,
                    FirestoreMapEncoding map_encoding) {
	if (IsFirestoreNull(firestore_value)) {
		FlatVector::SetNull(vector, index, true);
		return;
	}

	// Get the actual vector type - use this instead of the passed type to avoid mismatches
	const LogicalType &actual_type = vector.GetType();

	// Convert the Firestore value using actual vector type
	Value converted = FirestoreValueToDuckDB(firestore_value, actual_type, map_encoding);

	// Handle type mismatch: Firestore documents can have inconsistent types
	// If the converted value type doesn't match the actual vector type, try to handle gracefully
	if (converted.type().id() != actual_type.id()) {
		FS_LOG_DEBUG("Type mismatch: converted=" + converted.type().ToString() +
		             ", expected=" + actual_type.ToString());

		// Try to cast/convert the value to the actual vector type
		switch (actual_type.id()) {
		case LogicalTypeId::VARCHAR: {
			// Everything can become a string
			auto str = converted.ToString();
			FlatVector::GetData<string_t>(vector)[index] = StringVector::AddString(vector, str);
			return;
		}
		case LogicalTypeId::BIGINT: {
			// Try to convert string to bigint
			if (converted.type().id() == LogicalTypeId::VARCHAR) {
				try {
					auto str = converted.GetValue<std::string>();
					FlatVector::GetData<int64_t>(vector)[index] = std::stoll(str);
					return;
				} catch (const std::exception &e) {
					FS_LOG_DEBUG("BIGINT conversion failed: " + std::string(e.what()));
					FlatVector::SetNull(vector, index, true);
					return;
				}
			}
			FlatVector::SetNull(vector, index, true);
			return;
		}
		case LogicalTypeId::DOUBLE: {
			// Try to convert string/int to double
			if (converted.type().id() == LogicalTypeId::VARCHAR) {
				try {
					auto str = converted.GetValue<std::string>();
					FlatVector::GetData<double>(vector)[index] = std::stod(str);
					return;
				} catch (const std::exception &e) {
					FS_LOG_DEBUG("DOUBLE conversion failed: " + std::string(e.what()));
					FlatVector::SetNull(vector, index, true);
					return;
				}
			} else if (converted.type().id() == LogicalTypeId::BIGINT) {
				FlatVector::GetData<double>(vector)[index] = static_cast<double>(converted.GetValue<int64_t>());
				return;
			}
			FlatVector::SetNull(vector, index, true);
			return;
		}
		case LogicalTypeId::TIMESTAMP: {
			// Try to convert string to timestamp
			if (converted.type().id() == LogicalTypeId::VARCHAR) {
				try {
					auto str = converted.GetValue<std::string>();
					// Try parsing as timestamp
					FlatVector::GetData<timestamp_t>(vector)[index] = Timestamp::FromString(str, false);
					return;
				} catch (const std::exception &e) {
					FS_LOG_DEBUG("TIMESTAMP conversion failed: " + std::string(e.what()));
					FlatVector::SetNull(vector, index, true);
					return;
				}
			}
			FlatVector::SetNull(vector, index, true);
			return;
		}
		default:
			// Can't convert - set null
			FlatVector::SetNull(vector, index, true);
			return;
		}
	}

	// Types match - set the value directly
	switch (actual_type.id()) {
	case LogicalTypeId::VARCHAR: {
		auto str = converted.GetValue<std::string>();
		FlatVector::GetData<string_t>(vector)[index] = StringVector::AddString(vector, str);
		break;
	}
	case LogicalTypeId::BIGINT:
		FlatVector::GetData<int64_t>(vector)[index] = converted.GetValue<int64_t>();
		break;
	case LogicalTypeId::INTEGER:
		FlatVector::GetData<int32_t>(vector)[index] = converted.GetValue<int32_t>();
		break;
	case LogicalTypeId::DOUBLE:
		FlatVector::GetData<double>(vector)[index] = converted.GetValue<double>();
		break;
	case LogicalTypeId::FLOAT:
		FlatVector::GetData<float>(vector)[index] = converted.GetValue<float>();
		break;
	case LogicalTypeId::BOOLEAN:
		FlatVector::GetData<bool>(vector)[index] = converted.GetValue<bool>();
		break;
	case LogicalTypeId::TIMESTAMP:
		FlatVector::GetData<timestamp_t>(vector)[index] = converted.GetValue<timestamp_t>();
		break;
	case LogicalTypeId::LIST: {
		// Handle LIST type - need to use ListVector operations
		auto list_size = ListValue::GetChildren(converted).size();
		auto list_entry = list_entry_t(ListVector::GetListSize(vector), list_size);
		FlatVector::GetData<list_entry_t>(vector)[index] = list_entry;

		// Append child values to the list vector's child
		auto &child_vector = ListVector::GetEntry(vector);
		auto current_size = ListVector::GetListSize(vector);
		ListVector::SetListSize(vector, current_size + list_size);
		ListVector::Reserve(vector, current_size + list_size);

		auto &child_values = ListValue::GetChildren(converted);
		auto child_type = ListType::GetChildType(actual_type);

		for (idx_t i = 0; i < list_size; i++) {
			auto &child_val = child_values[i];
			if (child_val.IsNull()) {
				FlatVector::SetNull(child_vector, current_size + i, true);
			} else {
				switch (child_type.id()) {
				case LogicalTypeId::VARCHAR: {
					auto str = child_val.GetValue<std::string>();
					FlatVector::GetData<string_t>(child_vector)[current_size + i] =
					    StringVector::AddString(child_vector, str);
					break;
				}
				case LogicalTypeId::BIGINT:
					FlatVector::GetData<int64_t>(child_vector)[current_size + i] = child_val.GetValue<int64_t>();
					break;
				case LogicalTypeId::DOUBLE:
					FlatVector::GetData<double>(child_vector)[current_size + i] = child_val.GetValue<double>();
					break;
				case LogicalTypeId::BOOLEAN:
					FlatVector::GetData<bool>(child_vector)[current_size + i] = child_val.GetValue<bool>();
					break;
				default:
					// Fallback to string
					auto str = child_val.ToString();
					FlatVector::GetData<string_t>(child_vector)[current_size + i] =
					    StringVector::AddString(child_vector, str);
					break;
				}
			}
		}
		break;
	}
	case LogicalTypeId::STRUCT: {
		// Handle STRUCT type (e.g., geoPointValue)
		auto &struct_children = StructValue::GetChildren(converted);
		auto &struct_vector = StructVector::GetEntries(vector);
		auto &child_types = StructType::GetChildTypes(actual_type);

		for (idx_t i = 0; i < struct_children.size() && i < struct_vector.size(); i++) {
			auto &child_vec = *struct_vector[i];
			auto &child_val = struct_children[i];

			if (child_val.IsNull()) {
				FlatVector::SetNull(child_vec, index, true);
			} else {
				switch (child_types[i].second.id()) {
				case LogicalTypeId::DOUBLE:
					FlatVector::GetData<double>(child_vec)[index] = child_val.GetValue<double>();
					break;
				case LogicalTypeId::VARCHAR: {
					auto str = child_val.GetValue<std::string>();
					FlatVector::GetData<string_t>(child_vec)[index] = StringVector::AddString(child_vec, str);
					break;
				}
				case LogicalTypeId::BIGINT:
					FlatVector::GetData<int64_t>(child_vec)[index] = child_val.GetValue<int64_t>();
					break;
				case LogicalTypeId::BOOLEAN:
					FlatVector::GetData<bool>(child_vec)[index] = child_val.GetValue<bool>();
					break;
				default:
					FlatVector::SetNull(child_vec, index, true);
					break;
				}
			}
		}
		break;
	}
	case LogicalTypeId::BLOB: {
		// Handle BLOB type (bytesValue). StringValue::Get reads the stored
		// octets; GetValue<std::string> would cast the blob to VARCHAR first,
		// which renders every byte above 0x7f as a \xNN escape and stored the
		// escape text as the value.
		const auto &blob_data = StringValue::Get(converted);
		FlatVector::GetData<string_t>(vector)[index] = StringVector::AddString(vector, blob_data);
		break;
	}
	case LogicalTypeId::ARRAY: {
		// Handle ARRAY type (vectorValue - Firestore vector embeddings)
		auto &child_vector = ArrayVector::GetEntry(vector);
		auto array_size = ArrayType::GetSize(actual_type);
		auto &child_values = ArrayValue::GetChildren(converted);

		for (idx_t i = 0; i < array_size; i++) {
			if (i < child_values.size()) {
				auto &child_val = child_values[i];
				if (child_val.IsNull()) {
					FlatVector::SetNull(child_vector, index * array_size + i, true);
				} else {
					FlatVector::GetData<double>(child_vector)[index * array_size + i] = child_val.GetValue<double>();
				}
			} else {
				// Dimension mismatch - pad with nulls
				FlatVector::SetNull(child_vector, index * array_size + i, true);
			}
		}
		break;
	}
	default:
		// For complex types, convert to string representation
		auto str = converted.ToString();
		FlatVector::GetData<string_t>(vector)[index] = StringVector::AddString(vector, str);
		break;
	}
}

// Helper to infer the element type of an array by sampling its elements
LogicalType InferArrayElementType(const std::vector<json> &document_fields, const std::string &field_name,
                                  idx_t sample_size) {
	std::map<std::string, int64_t> element_type_counts;
	idx_t count = 0;

	for (const auto &fields : document_fields) {
		if (count >= sample_size)
			break;

		if (fields.contains(field_name) && fields[field_name].contains("arrayValue")) {
			const auto &arr = fields[field_name]["arrayValue"];
			if (arr.contains("values")) {
				for (const auto &elem : arr["values"]) {
					std::string elem_type = GetFirestoreTypeName(elem);
					if (elem_type != "nullValue") {
						element_type_counts[elem_type]++;
					}
				}
			}
		}
		count++;
	}

	// Find most common element type
	std::string best_type = "stringValue"; // Default to VARCHAR
	int64_t best_count = 0;
	for (const auto &[type_name, cnt] : element_type_counts) {
		if (cnt > best_count) {
			best_count = cnt;
			best_type = type_name;
		}
	}

	// Convert to DuckDB type (but not LIST - we want the element type)
	if (best_type == "stringValue")
		return LogicalType::VARCHAR;
	if (best_type == "integerValue")
		return LogicalType::BIGINT;
	if (best_type == "doubleValue")
		return LogicalType::DOUBLE;
	if (best_type == "booleanValue")
		return LogicalType::BOOLEAN;
	if (best_type == "timestampValue")
		return LogicalType::TIMESTAMP;
	// For complex types, fall back to VARCHAR
	return LogicalType::VARCHAR;
}

std::vector<InferredColumn> InferSchemaFromDocuments(const std::vector<json> &document_fields, idx_t sample_size) {
	std::map<std::string, std::map<std::string, int64_t>> field_type_counts;
	std::map<std::string, int64_t> field_occurrences;

	idx_t count = 0;
	for (const auto &fields : document_fields) {
		if (count >= sample_size)
			break;

		for (auto it = fields.begin(); it != fields.end(); ++it) {
			const std::string &field_name = it.key();
			const json &field_value = it.value();

			std::string type_name = GetFirestoreTypeName(field_value);

			field_type_counts[field_name][type_name]++;
			field_occurrences[field_name]++;
		}
		count++;
	}

	std::vector<InferredColumn> result;
	idx_t total_docs = std::min(static_cast<idx_t>(document_fields.size()), sample_size);

	for (const auto &[field_name, type_counts] : field_type_counts) {
		InferredColumn col;
		col.name = field_name;
		col.occurrence_count = field_occurrences[field_name];
		col.nullable = (col.occurrence_count < static_cast<int64_t>(total_docs));

		// Find most common type
		std::string best_type = "stringValue";
		int64_t best_count = 0;
		for (const auto &[type_name, cnt] : type_counts) {
			if (type_name != "nullValue" && cnt > best_count) {
				best_count = cnt;
				best_type = type_name;
			}
		}

		// For arrays, infer the element type
		if (best_type == "arrayValue") {
			LogicalType element_type = InferArrayElementType(document_fields, field_name, sample_size);
			col.type = LogicalType::LIST(element_type);
			FS_LOG_DEBUG("Array field '" + field_name + "' inferred element type: " + element_type.ToString());
		} else if (best_type == "vectorValue") {
			// For vectors, determine dimension N from the first non-null vector
			idx_t dimension = 0;
			for (const auto &fields : document_fields) {
				if (fields.contains(field_name) && IsFirestoreVector(fields[field_name])) {
					dimension = FirestoreVectorDimension(fields[field_name]);
					if (dimension > 0)
						break;
				}
			}
			if (dimension > 0) {
				col.type = LogicalType::ARRAY(LogicalType::DOUBLE, dimension);
				FS_LOG_DEBUG("Vector field '" + field_name + "' inferred dimension: " + std::to_string(dimension));
			} else {
				// Fallback if no dimension could be determined
				col.type = LogicalType::LIST(LogicalType::DOUBLE);
				FS_LOG_DEBUG("Vector field '" + field_name + "' could not determine dimension, using LIST(DOUBLE)");
			}
		} else {
			col.type = FirestoreTypeToDuckDB(best_type);
		}

		result.push_back(col);
	}

	FS_LOG_DEBUG("Inferred " + std::to_string(result.size()) + " columns from " + std::to_string(total_docs) +
	             " documents");

	return result;
}

} // namespace duckdb
