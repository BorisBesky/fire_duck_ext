#pragma once

#include <string>

namespace duckdb {

// Count the number of slash-delimited path segments, ignoring repeated,
// leading, and trailing separators.
int CountFirestorePathSegments(const std::string &path);

// Firestore document paths have an even number of segments, such as
// "users/uid" or "users/uid/orders/order_id".
bool IsFirestoreDocumentPath(const std::string &path);

// Collection-group scans use a leading '~' prefix, so they are never treated
// as document-path scans even if the remaining text happens to have even segments.
bool IsFirestoreDocumentPathCollection(const std::string &collection);

} // namespace duckdb
