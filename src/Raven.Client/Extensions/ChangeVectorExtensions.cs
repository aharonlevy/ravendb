﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json;

namespace Raven.Client.Extensions
{
    public static class ChangeVectorExtensions
    {              
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Dictionary<Guid, long> ToDictionary(this ChangeVectorEntry[] changeVector)
        {
            return changeVector.ToDictionary(x => x.DbId, x => x.Etag);
        }

        public static bool EqualTo(this ChangeVectorEntry[] self, ChangeVectorEntry[] other)
        {
            if (self.Length != other.Length)
                return false;

            for (int i = 0; i < self.Length; i++)
            {
                var otherEntry = other.FirstOrDefault(x => x.DbId == self[i].DbId);
                if (otherEntry.DbId == Guid.Empty) //not fount relevant entry
                    return false;

                if (self[i].Etag != otherEntry.Etag)
                    return false;
            }

            return true;
        }

        public static bool GreaterThan(this ChangeVectorEntry[] self, Dictionary<Guid, long> other)
        {
            for (int i = 0; i < self.Length; i++)
            {
                long otherEtag;
                if (other.TryGetValue(self[i].DbId, out otherEtag) == false)
                    return true;
                if (self[i].Etag > otherEtag)
                    return true;
            }
            return false;
        }

        public static bool GreaterThan(this ChangeVectorEntry[] self, ChangeVectorEntry[] other)
        {
            for (int i = 0; i < self.Length; i++)
            {
                var indexOfDbId = IndexOf(self[i].DbId, other);
                if (indexOfDbId == -1)
                    return true;
                if (self[i].Etag > other[indexOfDbId].Etag)
                    return true;
            }
            return false;
        }

        private static int IndexOf(Guid dbId, ChangeVectorEntry[] v)
        {
            for (int i = 0; i < v.Length; i++)
            {
                if (v[i].DbId == dbId)
                    return i;
            }
            return -1;
        }

        public static string Format(this ChangeVectorEntry[] changeVector, int? maxCount = null)
        {
            var max = maxCount ?? changeVector.Length;
            if (max == 0)
                return "[]";
            var sb = new StringBuilder();
            sb.Append("[");
            for (int i = 0; i < max; i++)
            {
                sb.Append(changeVector[i].DbId)
                    .Append(" : ")
                    .Append(changeVector[i].Etag)
                    .Append(", ");
            }
            sb.Length -= 2;
            sb.Append("]");
            return sb.ToString();
        }

        public static ChangeVectorEntry[] ToVector(this BlittableJsonReaderArray vectorJson)
        {
            var result = new ChangeVectorEntry[vectorJson.Length];
            int iter = 0;
            foreach (BlittableJsonReaderObject entryJson in vectorJson)
            {
                if (!entryJson.TryGet(nameof(ChangeVectorEntry.DbId), out result[iter].DbId))
                    throw new InvalidDataException("Tried to find " + nameof(ChangeVectorEntry.DbId) + " property in change vector, but didn't find.");
                if (!entryJson.TryGet(nameof(ChangeVectorEntry.Etag), out result[iter].Etag))
                    throw new InvalidDataException("Tried to find " + nameof(ChangeVectorEntry.Etag) + " property in change vector, but didn't find.");

                iter++;
            }
            return result;
        }

        public static ChangeVectorEntry[] ToChangeVector(this string str)
        {
            if (str == null)
            {
                return null;
            }

            var entries = str.Split(',');
            var changeVector = new ChangeVectorEntry[entries.Length];
            for (var index = 0; index < entries.Length; index++)
            {
                changeVector[index] = ChangeVectorEntry.FromString(entries[index]);
            }
            return changeVector;
        }

        public static string ToJson(this ChangeVectorEntry[] self)
        {
            if (self == null)
                return null;

            var sb = new StringBuilder();
            for (int i = 0; i < self.Length; i++)
            {
                if (i != 0)
                    sb.Append(", ");
                self[i].Append(sb);
            }
            return sb.ToString();
        }

        public static void ToBase26(StringBuilder sb, int tag)
        {
            do
            {
                var reminder = tag % 26;
                sb.Append((char)('A' + reminder));
                tag /= 26;
            } while (tag != 0);
        }

        public static int FromBase26(string tag)
        {
            //TODO: validation of valid chars
            var val = 0;
            for (int i = 0; i < tag.Length; i++)
            {
                val *= 26;
                val += (tag[i] - 'A');
            }
            return val;
        }
    }
}