#include "firestore_path_utils.hpp"

#include <vector>

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

bool IsFirestoreCollectionGroup(const std::string &collection) {
	return !collection.empty() && collection[0] == '~';
}

// Split on every '/', keeping empty segments so that a path such as
// "users//orders/o1" can be refused rather than silently read as four segments.
static std::vector<std::string> SplitPathSegments(const std::string &path) {
	std::vector<std::string> segments;
	size_t start = 0;
	while (true) {
		size_t slash = path.find('/', start);
		if (slash == std::string::npos) {
			segments.push_back(path.substr(start));
			return segments;
		}
		segments.push_back(path.substr(start, slash - start));
		start = slash + 1;
	}
}

std::string FirestoreCollectionGroupNameError(const std::string &collection_group) {
	const std::string group_id =
	    IsFirestoreCollectionGroup(collection_group) ? collection_group.substr(1) : collection_group;
	if (group_id.empty()) {
		return "collection group '" + collection_group + "' names no collection; write it as '~' followed by a " +
		       "collection id, such as '~orders'";
	}
	size_t last_slash = group_id.rfind('/');
	if (last_slash != std::string::npos) {
		std::string suggestion = group_id.substr(last_slash + 1);
		if (suggestion.empty()) {
			suggestion = "orders";
		}
		return "collection group '" + collection_group + "' is a path; a collection group is a single collection " +
		       "id, such as '~" + suggestion + "'";
	}
	return "";
}

std::string ResolveFirestoreCollectionGroupDocumentPath(const std::string &collection_group,
                                                        const std::string &document_path, std::string &resolved_path) {
	resolved_path.clear();

	auto name_error = FirestoreCollectionGroupNameError(collection_group);
	if (!name_error.empty()) {
		return name_error;
	}
	const std::string group_id =
	    IsFirestoreCollectionGroup(collection_group) ? collection_group.substr(1) : collection_group;
	const std::string example = "'<parent path>/" + group_id + "/<document id>'";

	std::string path = document_path;
	if (!path.empty() && path[0] == '/') {
		path = path.substr(1);
	}
	if (path.empty()) {
		return "document path is empty; writes to collection group '" + collection_group +
		       "' need each document's full path, such as " + example;
	}

	auto segments = SplitPathSegments(path);
	for (const auto &segment : segments) {
		if (segment.empty()) {
			return "document path '" + document_path + "' has an empty segment";
		}
	}
	if (segments.size() % 2 != 0) {
		return "'" + document_path + "' is not a full document path; writes to collection group '" + collection_group +
		       "' need each document's full path, such as " + example + " (the __document_id " +
		       "that firestore_scan('" + collection_group + "') returns)";
	}

	// A document's collection is the segment before its id.
	const std::string &collection_id = segments[segments.size() - 2];
	if (collection_id != group_id) {
		return "document '" + document_path + "' is in collection '" + collection_id + "', not in collection group '" +
		       collection_group + "'";
	}

	resolved_path = path;
	return "";
}

} // namespace duckdb
