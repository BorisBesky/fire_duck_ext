#include "firestore_path_utils.hpp"

namespace duckdb {

int CountFirestorePathSegments(const std::string &path) {
	int count = 0;
	bool in_segment = false;
	for (char c : path) {
		if (c == '/') {
			in_segment = false;
		} else if (!in_segment) {
			in_segment = true;
			count++;
		}
	}
	return count;
}

bool IsFirestoreDocumentPath(const std::string &path) {
	int segments = CountFirestorePathSegments(path);
	return segments >= 2 && segments % 2 == 0;
}

bool IsFirestoreDocumentPathCollection(const std::string &collection) {
	if (!collection.empty() && collection[0] == '~') {
		return false;
	}
	return IsFirestoreDocumentPath(collection);
}

void SplitFirestoreCollectionPath(const std::string &collection, bool is_collection_group, std::string &parent_path,
                                  std::string &collection_id) {
	parent_path.clear();
	collection_id = collection;

	if (is_collection_group) {
		// Collection-group queries run from the database root with allDescendants=true;
		// the leading '~' marker is not part of the collection id.
		if (!collection_id.empty() && collection_id[0] == '~') {
			collection_id = collection_id.substr(1);
		}
		return;
	}

	// Nested subcollection (e.g. "users/uid/orders"): runQuery must target the parent
	// document path ("users/uid") with from.collectionId set to the final segment only
	// ("orders"). Top-level collections have no parent.
	size_t last_slash = collection_id.rfind('/');
	if (last_slash != std::string::npos) {
		parent_path = collection_id.substr(0, last_slash);
		collection_id = collection_id.substr(last_slash + 1);
	}
}

} // namespace duckdb
