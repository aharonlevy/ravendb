﻿using System;
using System.Collections.Generic;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Raven.Client.Documents.Indexes.TimeSeries;

namespace Raven.Server.Documents.Indexes.Static.TimeSeries
{
    public sealed class TimeSeriesSegmentObjectInstance : ObjectInstance
    {
        private readonly DynamicTimeSeriesSegment _segment;

        private Dictionary<JsValue, PropertyDescriptor> _properties = new Dictionary<JsValue, PropertyDescriptor>();

        public TimeSeriesSegmentObjectInstance(Engine engine, DynamicTimeSeriesSegment segment) : base(engine)
        {
            _segment = segment ?? throw new ArgumentNullException(nameof(segment));

            SetPrototypeOf(engine.Object.PrototypeObject);
        }

        public override bool Delete(JsValue property)
        {
            throw new NotSupportedException();
        }

        public override PropertyDescriptor GetOwnProperty(JsValue property)
        {
            if (_properties.TryGetValue(property, out var value) == false)
                _properties[property] = value = GetPropertyValue(property);

            return value;
        }

        private PropertyDescriptor GetPropertyValue(JsValue property)
        {
            if (property == nameof(DynamicTimeSeriesSegment.Entries))
                return new TimeSeriesSegmentEntriesPropertyDescriptor(Engine, _segment);

            if (property == nameof(TimeSeriesSegment.DocumentId))
                return new PropertyDescriptor(_segment._segmentEntry.DocId.ToString(), writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.Name))
                return new PropertyDescriptor(_segment._segmentEntry.Name.ToString(), writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.Count))
                return new PropertyDescriptor(_segment.Count, writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.End))
                return new PropertyDescriptor(_engine.Date.Construct(_segment.End), writable: false, enumerable: false, configurable: false);

            if (property == nameof(DynamicTimeSeriesSegment.Start))
                return new PropertyDescriptor(_engine.Date.Construct(_segment.Start), writable: false, enumerable: false, configurable: false);

            return PropertyDescriptor.Undefined;
        }

        public override bool Set(JsValue property, JsValue value, JsValue receiver)
        {
            throw new NotSupportedException();
        }

        public override IEnumerable<KeyValuePair<JsValue, PropertyDescriptor>> GetOwnProperties()
        {
            throw new NotSupportedException();
        }

        public override List<JsValue> GetOwnPropertyKeys(Types types = Types.String | Types.Symbol)
        {
            throw new NotSupportedException();
        }

        private class TimeSeriesSegmentEntriesPropertyDescriptor : PropertyDescriptor
        {
            private readonly ArrayInstance _value;

            public TimeSeriesSegmentEntriesPropertyDescriptor(Engine engine, DynamicTimeSeriesSegment segment)
                : base(PropertyFlag.CustomJsValue | PropertyFlag.Writable | PropertyFlag.WritableSet | PropertyFlag.Enumerable | PropertyFlag.EnumerableSet)
            {
                _value = CreateValue(engine, segment);
            }

            public override JsValue Get => CustomValue;

            public override JsValue Set => throw new NotSupportedException();

            protected override JsValue CustomValue
            {
                get => _value;
                set
                {
                    throw new NotSupportedException();
                }
            }

            private static ArrayInstance CreateValue(Engine engine, DynamicTimeSeriesSegment segment)
            {
                var items = new PropertyDescriptor[segment._segmentEntry.Segment.NumberOfLiveEntries];
                var i = 0;
                foreach (DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry in segment.Entries)
                {
                    items[i] = new TimeSeriesSegmentEntryPropertyDescriptor(engine, entry);
                    i++;
                }

                var jsArray = new ArrayInstance(engine, items);
                jsArray.SetPrototypeOf(engine.Array.PrototypeObject);

                return jsArray;
            }
        }

        private class TimeSeriesSegmentEntryPropertyDescriptor : PropertyDescriptor
        {
            private readonly ObjectInstance _value;

            public TimeSeriesSegmentEntryPropertyDescriptor(Engine engine, DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry)
                : base(PropertyFlag.CustomJsValue | PropertyFlag.Writable | PropertyFlag.WritableSet | PropertyFlag.Enumerable | PropertyFlag.EnumerableSet)
            {
                _value = CreateValue(engine, entry);
            }

            public override JsValue Get => CustomValue;

            public override JsValue Set => throw new NotSupportedException();

            protected override JsValue CustomValue
            {
                get => _value;
                set
                {
                    throw new NotSupportedException();
                }
            }

            private static ObjectInstance CreateValue(Engine engine, DynamicTimeSeriesSegment.DynamicTimeSeriesEntry entry)
            {
                var value = new ObjectInstance(engine);

                value.Set(nameof(entry.Tag), entry._entry.Tag?.ToString());
                value.Set(nameof(entry.Timestamp), engine.Date.Construct(entry._entry.Timestamp));

                var values = new JsValue[entry._entry.Values.Length];
                for (var i = 0; i < values.Length; i++)
                    values[i] = entry._entry.Values.Span[i];

                var array = engine.Array.Construct(values.Length);
                engine.Array.PrototypeObject.Push(array, values);

                value.Set(nameof(entry.Value), values[0]);
                value.Set(nameof(entry.Values), array);

                value.SetPrototypeOf(engine.Object.PrototypeObject);

                return value;
            }
        }
    }
}
