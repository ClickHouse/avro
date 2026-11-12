/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Generic.hh"
#include "NodeImpl.hh"
#include <algorithm>
#include <sstream>

namespace avro {

using std::string;
using std::vector;
using std::ostringstream;

typedef vector<uint8_t> bytes;

namespace {

/// The reader may create this many `GenericDatum` objects (or the equivalent in bytes of variable-size values) from a
/// stretch of the input that does not advance. Items that consume no bytes (`null`, an empty record, a record made of
/// defaults, defaults of any shape) are not bounded by the size of the input, so this budget is the only thing that
/// bounds them.
constexpr size_t maxZeroByteDatumNodes = size_t(1) << 16;

/// The number of `GenericDatum` objects that constructing `GenericDatum(node, ShallowTag())` creates: itself and, for a
/// union, the shallow datum of its first branch. A shallow record has none of its fields and an array or a map is
/// empty, so the number does not depend on the datums that the schema describes below the datum.
size_t shallowDatumCount(const NodePtr& node)
{
    return (node->type() == AVRO_SYMBOLIC ? resolveSymbol(node) : node)->type() == AVRO_UNION ? 2 : 1;
}

/// The number of datums that a string, bytes or key of `bytes` bytes is charged as.
size_t datumsOfBytes(size_t bytes) {
    return (bytes + sizeof(GenericDatum) - 1) / sizeof(GenericDatum);
}

}   // namespace

/// What a read constructs is charged to a budget before it is constructed, and the budget refills whenever the input
/// advances, because what follows is backed by the input again.
///
/// The read constructs every datum shallow (`ShallowTag`) and charges each as the datums that its construction creates
/// (`shallowDatumCount`): the item of an array, the key and the value of an entry of a map, the datum of a union branch
/// that it selects, and a field of a shallow record, which it constructs immediately before it reads it. A string, bytes
/// or fixed that is decoded without the input advancing is charged by its size, as `ceil(size / sizeof(GenericDatum))`
/// datums, after it is decoded.
///
/// So, from a root that is shallow (`GenericReader::read` constructs it, and so does a caller that passes
/// `ShallowTag`), a stretch of the input that does not advance constructs at most `maxZeroByteDatumNodes` datums,
/// however arrays, maps, records and defaults are nested, and neither an item, a union branch, a field nor a default is
/// expanded to the datums of its schema before the input backs them. A value that the input backs is not rejected for
/// the number of datums that it holds: the fields of a record are charged one at a time, and a field that is read from
/// input that advances refills the budget for the next.
///
/// What this does not bound: the root that the caller constructs without `ShallowTag`, which constructs all of the datums
/// of its schema before anything is read, as it always did; the datums that such a root holds, which the read reuses
/// and does not charge; the size of the value, which may have `maxZeroByteDatumNodes` datums for each byte of the input
/// that advanced; the memory of a datum, which is `sizeof(GenericDatum)` and, for a record, a union, an array or a map,
/// also the heap block of its value, so that a charged datum is roughly 70 bytes for the datums that the tests create;
/// the capacity of the vectors of items, entries, fields and early fields, which may be twice what is charged; the null
/// placeholders of the fields that a resolved record reads early, which are as many as the early fields that were
/// charged; the vector of the order of the fields of a record that is resolved, which has one entry per field; the
/// schema and the defaults that it holds, which are as large as its text; and a string, which is decoded before it is
/// charged, in blocks that grow with the bytes that the input holds.
struct GenericReader::ZeroByteBudget {
    uint64_t position;
    size_t datums = 0;

    explicit ZeroByteBudget(const Decoder& d) : position(d.inputPosition()) {}

    /// Charges `count` datums that are created from the current position of the input.
    void charge(const Decoder& d, size_t count) {
        const uint64_t now = d.inputPosition();
        if (now != position) {
            position = now;
            datums = 0;
        }
        if (count > maxZeroByteDatumNodes - datums) {
            throw Exception(boost::format("Values that take no bytes exceed the limit of %1% datums") % maxZeroByteDatumNodes);
        }
        datums += count;
    }

    /// Charges a string, bytes or key of `bytes` bytes that was decoded from `before`, unless decoding consumed input.
    void chargeBytes(const Decoder& d, uint64_t before, size_t bytes) {
        if (d.inputPosition() == before) {
            charge(d, datumsOfBytes(bytes));
        }
    }
};

void GenericContainer::assertType(const NodePtr& schema, Type type) {
    if (schema->type() != type) {
        throw Exception(boost::format("Schema type %1 expected %2") %
            toString(schema->type()) % toString(type));
    }
}

GenericReader::GenericReader(const ValidSchema& s, const DecoderPtr& decoder) :
    schema_(s), isResolving_(dynamic_cast<ResolvingDecoder*>(&(*decoder)) != 0),
    decoder_(decoder)
{
}

GenericReader::GenericReader(const ValidSchema& writerSchema,
    const ValidSchema& readerSchema, const DecoderPtr& decoder) :
    schema_(readerSchema),
    isResolving_(true),
    decoder_(resolvingDecoder(writerSchema, readerSchema, decoder))
{
}

void GenericReader::read(GenericDatum& datum) const
{
    datum = GenericDatum(schema_, ShallowTag());
    ZeroByteBudget budget(*decoder_);
    read(datum, *decoder_, isResolving_, 0, budget);
}

void GenericReader::read(GenericDatum& datum, Decoder& d, bool isResolving, size_t depth, ZeroByteBudget& budget)
{
    if (depth >= GenericDatum::maxNestingDepth) {
        throw Exception(boost::format("Cannot read a datum nested deeper than %1% levels") % GenericDatum::maxNestingDepth);
    }
    if (datum.isUnion()) {
        const size_t branch = d.decodeUnionIndex();
        if (branch != datum.unionBranch()) {
            // Selecting another branch constructs its datum, which replaces the one of the previous branch, and which
            // takes no input to construct.
            budget.charge(d, shallowDatumCount(datum.branchSchema(branch)));
            datum.selectBranch(branch, ShallowTag());
        }
    }
    switch (datum.type()) {
    case AVRO_NULL:
        d.decodeNull();
        break;
    case AVRO_BOOL:
        datum.value<bool>() = d.decodeBool();
        break;
    case AVRO_INT:
        datum.value<int32_t>() = d.decodeInt();
        break;
    case AVRO_LONG:
        datum.value<int64_t>() = d.decodeLong();
        break;
    case AVRO_FLOAT:
        datum.value<float>() = d.decodeFloat();
        break;
    case AVRO_DOUBLE:
        datum.value<double>() = d.decodeDouble();
        break;
    case AVRO_STRING:
        {
            const uint64_t positionBefore = d.inputPosition();
            std::string& s = datum.value<string>();
            d.decodeString(s);
            budget.chargeBytes(d, positionBefore, s.size());
        }
        break;
    case AVRO_BYTES:
        {
            const uint64_t positionBefore = d.inputPosition();
            bytes& b = datum.value<bytes>();
            d.decodeBytes(b);
            budget.chargeBytes(d, positionBefore, b.size());
        }
        break;
    case AVRO_FIXED:
        {
            const uint64_t positionBefore = d.inputPosition();
            GenericFixed& f = datum.value<GenericFixed>();
            d.decodeFixed(f.schema()->fixedSize(), f.value());
            budget.chargeBytes(d, positionBefore, f.value().size());
        }
        break;
    case AVRO_RECORD:
        {
            GenericRecord& r = datum.value<GenericRecord>();
            const size_t c = r.schema()->leaves();
            std::vector<size_t> fo;
            if (isResolving) {
                fo = static_cast<ResolvingDecoder&>(d).fieldOrder();
            }
            // The fields of a shallow record are constructed one at a time as they are read, each charged when it is
            // constructed. A field that the resolving decoder yields before the fields that precede it in the reader's
            // schema is read into a datum of its own, which is moved into the record when all of its fields are read, so
            // that a field that is not read yet has no placeholder, which no input backs. The fields of a record that
            // was not constructed shallow are all there, and are read in place.
            std::vector<GenericDatum>& fields = r.fields_;
            std::vector<std::pair<size_t, GenericDatum>> early;
            for (size_t i = 0; i < c; ++i) {
                const size_t pos = isResolving ? fo[i] : i;
                if (pos < fields.size()) {
                    read(fields[pos], d, isResolving, depth + 1, budget);
                } else if (pos == fields.size()) {
                    budget.charge(d, shallowDatumCount(r.schema()->leafAt(pos)));
                    fields.emplace_back(r.schema()->leafAt(pos), ShallowTag());
                    read(fields.back(), d, isResolving, depth + 1, budget);
                } else {
                    budget.charge(d, shallowDatumCount(r.schema()->leafAt(pos)));
                    early.emplace_back(pos, GenericDatum(r.schema()->leafAt(pos), ShallowTag()));
                    read(early.back().second, d, isResolving, depth + 1, budget);
                }
            }
            if (!early.empty()) {
                // The positions that are not in the record are the ones that were read early, and each of them was charged.
                fields.resize(c);
                for (auto& [pos, field] : early) {
                    fields[pos] = std::move(field);
                }
            }
        }
        break;
    case AVRO_ENUM:
        datum.value<GenericEnum>().set(d.decodeEnum());
        break;
    case AVRO_ARRAY:
        {
            GenericArray& v = datum.value<GenericArray>();
            vector<GenericDatum>& r = v.value();
            const NodePtr& nn = v.schema()->leafAt(0);
            const size_t itemNodes = shallowDatumCount(nn);
            r.resize(0);
            for (size_t m = d.arrayStart(); m != 0; m = d.arrayNext()) {
                // Grow with the decoded items, not with the block count, which comes from the file.
                for (size_t i = 0; i < m; ++i) {
                    budget.charge(d, itemNodes);
                    r.emplace_back(nn, ShallowTag());
                    read(r.back(), d, isResolving, depth + 1, budget);
                }
            }
        }
        break;
    case AVRO_MAP:
        {
            GenericMap& v = datum.value<GenericMap>();
            GenericMap::Value& r = v.value();
            const NodePtr& nn = v.schema()->leafAt(1);
            // An entry is its key and the datums of its value.
            const size_t entryNodes = 1 + shallowDatumCount(nn);
            r.resize(0);
            for (size_t m = d.mapStart(); m != 0; m = d.mapNext()) {
                // Grow with the decoded items, not with the block count, which comes from the file.
                for (size_t i = 0; i < m; ++i) {
                    budget.charge(d, entryNodes);
                    r.emplace_back();
                    const uint64_t positionBefore = d.inputPosition();
                    d.decodeString(r.back().first);
                    budget.chargeBytes(d, positionBefore, r.back().first.size());
                    r.back().second = GenericDatum(nn, ShallowTag());
                    read(r.back().second, d, isResolving, depth + 1, budget);
                }
            }
        }
        break;
    default:
        throw Exception(boost::format("Unknown schema type %1%") %
            toString(datum.type()));
    }
}

void GenericReader::read(Decoder& d, GenericDatum& g, const ValidSchema& s)
{
    g = GenericDatum(s, ShallowTag());
    read(d, g);
}

void GenericReader::read(Decoder& d, GenericDatum& g)
{
    ZeroByteBudget budget(d);
    read(g, d, dynamic_cast<ResolvingDecoder*>(&d) != 0, 0, budget);
}

GenericWriter::GenericWriter(const ValidSchema& s, const EncoderPtr& encoder) :
    schema_(s), encoder_(encoder)
{
}

void GenericWriter::write(const GenericDatum& datum) const
{
    write(datum, *encoder_);
}

void GenericWriter::write(const GenericDatum& datum, Encoder& e)
{
    if (datum.isUnion()) {
        e.encodeUnionIndex(datum.unionBranch());
    }
    switch (datum.type()) {
    case AVRO_NULL:
        e.encodeNull();
        break;
    case AVRO_BOOL:
        e.encodeBool(datum.value<bool>());
        break;
    case AVRO_INT:
        e.encodeInt(datum.value<int32_t>());
        break;
    case AVRO_LONG:
        e.encodeLong(datum.value<int64_t>());
        break;
    case AVRO_FLOAT:
        e.encodeFloat(datum.value<float>());
        break;
    case AVRO_DOUBLE:
        e.encodeDouble(datum.value<double>());
        break;
    case AVRO_STRING:
        e.encodeString(datum.value<string>());
        break;
    case AVRO_BYTES:
        e.encodeBytes(datum.value<bytes>());
        break;
    case AVRO_FIXED:
        {
            const GenericFixed& f = datum.value<GenericFixed>();
            if (f.value().size() != f.schema()->fixedSize()) {
                throw Exception("Value of fixed has a size that differs from the size of its schema");
            }
            e.encodeFixed(f.value());
        }
        break;
    case AVRO_RECORD:
        {
            const GenericRecord& r = datum.value<GenericRecord>();
            size_t c = r.schema()->leaves();
            if (r.fieldCount() != c) {
                // A record that was constructed shallow and was not read to the end, as when its read threw.
                throw Exception("Record has fewer fields than its schema");
            }
            for (size_t i = 0; i < c; ++i) {
                write(r.fieldAt(i), e);
            }
        }
        break;
    case AVRO_ENUM:
        e.encodeEnum(datum.value<GenericEnum>().value());
        break;
    case AVRO_ARRAY:
        {
            const GenericArray::Value& r = datum.value<GenericArray>().value();
            e.arrayStart();
            if (! r.empty()) {
                e.setItemCount(r.size());
                for (GenericArray::Value::const_iterator it = r.begin();
                    it != r.end(); ++it) {
                    e.startItem();
                    write(*it, e);
                }
            }
            e.arrayEnd();
        }
        break;
    case AVRO_MAP:
        {
            const GenericMap::Value& r = datum.value<GenericMap>().value();
            e.mapStart();
            if (! r.empty()) {
                e.setItemCount(r.size());
                for (GenericMap::Value::const_iterator it = r.begin();
                    it != r.end(); ++it) {
                    e.startItem();
                    e.encodeString(it->first);
                    write(it->second, e);
                }
            }
            e.mapEnd();
        }
        break;
    default:
        throw Exception(boost::format("Unknown schema type %1%") %
            toString(datum.type()));
    }
}

void GenericWriter::write(Encoder& e, const GenericDatum& g)
{
    write(g, e);
}

}   // namespace avro
