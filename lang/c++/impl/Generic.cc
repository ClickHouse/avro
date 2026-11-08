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
/// stretch of the input that does not advance, which is 2 MiB of 32-byte datums. Items that consume no bytes (`null`,
/// an empty record, a record made of defaults, defaults of any shape) are not bounded by the size of the input, so
/// this budget is the only thing that bounds them.
constexpr size_t maxZeroByteDatumNodes = size_t(1) << 16;

/// The number of `GenericDatum` objects that `GenericDatum(node)` constructs eagerly, including itself: a record
/// constructs one datum per field recursively, a union constructs the datum of its selected branch (the largest
/// branch is counted, because reading may select any of them). The result saturates at `maxZeroByteDatumNodes + 1`.
size_t datumNodeCount(const NodePtr& node, size_t depth = 0) {
    constexpr size_t saturated = maxZeroByteDatumNodes + 1;
    if (depth >= GenericDatum::maxNestingDepth) {
        return saturated;
    }
    switch (node->type()) {
    case AVRO_SYMBOLIC:
        return datumNodeCount(resolveSymbol(node), depth + 1);
    case AVRO_RECORD: {
        size_t count = 1;
        for (size_t i = 0; i < node->leaves(); ++i) {
            count += datumNodeCount(node->leafAt(i), depth + 1);
            if (count >= saturated) {
                return saturated;
            }
        }
        return count;
    }
    case AVRO_UNION:
        // `GenericUnion` constructs only its first branch; others are built by `selectBranch` while reading.
        return std::min(saturated, 1 + (node->leaves() > 0 ? datumNodeCount(node->leafAt(0), depth + 1) : 0));
    default:
        return 1;
    }
}

/// The number of `GenericDatum` objects in a datum that was just constructed or read, excluding the elements of arrays
/// and maps, which are charged as they are appended. Unlike `datumNodeCount`, it accounts for the branches that reading
/// selected.
size_t materializedDatumCount(const GenericDatum& datum) {
    // A union is a datum wrapping the datum of its selected branch.
    size_t count = datum.isUnion() ? 2 : 1;
    if (datum.type() == AVRO_RECORD) {
        const GenericRecord& record = datum.value<GenericRecord>();
        for (size_t i = 0; i < record.fieldCount() && count <= maxZeroByteDatumNodes; ++i) {
            count += materializedDatumCount(record.fieldAt(i));
        }
    }
    return count;
}

/// The number of datums that a string, bytes or key of `bytes` bytes is charged as.
size_t datumsOfBytes(size_t bytes) {
    return (bytes + sizeof(GenericDatum) - 1) / sizeof(GenericDatum);
}

}   // namespace

/// Every array item, map entry, selected union branch and string that reading creates is charged to the budget, and the
/// budget refills whenever the input advances, because what follows is backed by the input again. What a stretch of the
/// input that does not advance can create (the items of a `null` array, a default of any shape, an empty record that is
/// given defaults) is therefore bounded by `maxZeroByteDatumNodes`, however the arrays, maps and records that create it
/// are nested, while a valid input is not rejected for the number of items that it backs.
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
    datum = GenericDatum(schema_.root());
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
            // Selecting another branch constructs its datum, which replaces the one of the previous branch.
            datum.selectBranch(branch);
            budget.charge(d, materializedDatumCount(datum) - 1);
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
            GenericFixed& f = datum.value<GenericFixed>();
            d.decodeFixed(f.schema()->fixedSize(), f.value());
        }
        break;
    case AVRO_RECORD:
        {
            GenericRecord& r = datum.value<GenericRecord>();
            size_t c = r.schema()->leaves();
            if (isResolving) {
                std::vector<size_t> fo =
                    static_cast<ResolvingDecoder&>(d).fieldOrder();
                for (size_t i = 0; i < c; ++i) {
                    read(r.fieldAt(fo[i]), d, isResolving, depth + 1, budget);
                }
            } else {
                for (size_t i = 0; i < c; ++i) {
                    read(r.fieldAt(i), d, isResolving, depth + 1, budget);
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
            const size_t itemNodes = datumNodeCount(nn);
            r.resize(0);
            for (size_t m = d.arrayStart(); m != 0; m = d.arrayNext()) {
                // Grow with the decoded items, not with the block count, which comes from the file.
                for (size_t i = 0; i < m; ++i) {
                    budget.charge(d, itemNodes);
                    r.emplace_back(nn);
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
            const size_t entryNodes = 1 + datumNodeCount(nn);
            r.resize(0);
            for (size_t m = d.mapStart(); m != 0; m = d.mapNext()) {
                // Grow with the decoded items, not with the block count, which comes from the file.
                for (size_t i = 0; i < m; ++i) {
                    budget.charge(d, entryNodes);
                    r.emplace_back();
                    const uint64_t positionBefore = d.inputPosition();
                    d.decodeString(r.back().first);
                    budget.chargeBytes(d, positionBefore, r.back().first.size());
                    r.back().second = GenericDatum(nn);
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
    g = GenericDatum(s);
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
        e.encodeFixed(datum.value<GenericFixed>().value());
        break;
    case AVRO_RECORD:
        {
            const GenericRecord& r = datum.value<GenericRecord>();
            size_t c = r.schema()->leaves();
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
