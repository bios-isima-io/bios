/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.isima.bios.data.impl.feature;

import static io.isima.bios.common.Constants.ORDER_BY_TIMESTAMP;

import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.data.impl.storage.CassAttributeDesc;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.errors.EventExtractError;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.Avg;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.req.Aggregate;
import io.isima.bios.req.MutableAggregate;
import io.isima.bios.req.View;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A helper class that generates extraction view function. */
public class ExtractView {
  private static final Logger logger = LoggerFactory.getLogger(ExtractView.class);

  /** Default view function that sorts the event by timestamp. */
  public static final Function<List<Event>, List<Event>> SORT_BY_TIMESTAMP =
      new Function<>() {
        @Override
        public List<Event> apply(List<Event> events) {
          Collections.sort(events, Comparators.TIMESTAMP);
          return events;
        }
      };

  public static final Function<List<Event>, List<Event>> REVERSE_SORT_BY_TIMESTAMP =
      new Function<>() {
        @Override
        public List<Event> apply(List<Event> events) {
          Collections.sort(events, Comparators.REVERSE_TIMESTAMP);
          return events;
        }
      };

  /** Default aggregate function that does not process the event list. */
  private static final Function<List<Event>, Object> TRANSPARENT_REDUCER =
      new Function<>() {
        @Override
        public List<Event> apply(List<Event> events) {
          return events;
        }
      };

  /**
   * Method to create a view function based on given view configuration and cassStream.
   *
   * @param cassStream Cassandra stream info.
   * @return View function
   * @throws ApplicationException when the method fails to create the view.
   */
  public static Function<List<Event>, List<Event>> createView(
      View view,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      CassStream cassStream,
      Supplier<Event> eventSupplier)
      throws ApplicationException {
    if (view == null) {
      return SORT_BY_TIMESTAMP;
    }
    switch (view.getFunction()) {
      case SORT:
        if (ORDER_BY_TIMESTAMP.equalsIgnoreCase(view.getBy())) {
          return view.getReverse() ? REVERSE_SORT_BY_TIMESTAMP : SORT_BY_TIMESTAMP;
        } else {
          return SORT_BY_TIMESTAMP.andThen(createSortFunction(view, cassStream));
        }
      case GROUP:
        final Function<List<Event>, List<Event>> groupFunction =
            createGroupFunction(view, attributes, aggregates, cassStream, eventSupplier);
        return groupFunction.andThen(createSortFunction(view, cassStream));
      default:
        throw new ApplicationException("Create view " + view.getFunction() + " is unsupported");
    }
  }

  public static Function<List<Event>, List<Event>> createSortFunction(
      View sortSpec, CassStream cassStream) {
    // Return an identity function if the spec is null
    if (sortSpec == null) {
      return (events) -> events;
    }
    Comparator<Event> comp = generateComparator(sortSpec, cassStream);
    return new Function<>() {
      @Override
      public List<Event> apply(List<Event> events) {
        Collections.sort(events, comp);
        return events;
      }
    };
  }

  public static Comparator<Event> generateComparator(View view, CassStream cassStream) {
    int polarity = view.getReverse() ? -1 : 1;
    boolean caseInsensitive = !view.getCaseSensitive();
    if (view.getDimensions() != null) {
      final List<Comparator<Event>> comparators = new ArrayList<>();
      for (String attribute : view.getDimensions()) {
        comparators.add(
            Comparators.generateComparator(
                cassStream.getAttributeDesc(attribute), polarity, caseInsensitive));
      }
      return new Comparator<>() {
        @Override
        public int compare(Event event1, Event event2) {
          for (Comparator<Event> comparator : comparators) {
            final int result = comparator.compare(event1, event2);
            if (result != 0) {
              return result;
            }
          }
          return 0;
        }
      };
    } else {
      AttributeDesc desc = cassStream.getAttributeDesc(view.getBy());
      if (desc == null) {
        return Comparators.generateComparator(
            view.getBy(), InternalAttributeType.LONG, polarity, caseInsensitive);
      } else {
        return Comparators.generateComparator(desc, polarity, caseInsensitive);
      }
    }
  }

  private static Function<List<Event>, List<Event>> createGroupFunction(
      View view,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      CassStream cassStream,
      Supplier<Event> eventSupplier) {

    final var calculator =
        new GroupCalculator(view, attributes, aggregates, cassStream, eventSupplier);

    return new Function<>() {
      @Override
      public List<Event> apply(List<Event> events) {
        final var ctx = calculator.start();
        events.forEach((record) -> calculator.consumeRecord(record, ctx));
        return calculator.finish(ctx);
      }
    };
  }

  @Getter
  public static class GroupCalculator {
    private final List<String> dimensions;
    private final List<String> outAttributes;
    private final List<SignalReducer> reducers;
    private final Supplier<Event> eventSupplier;

    public GroupCalculator(
        View view,
        List<String> attributes,
        List<? extends Aggregate> aggregates,
        CassStream cassStream,
        Supplier<Event> eventSupplier) {
      outAttributes = new ArrayList<>();
      reducers = new ArrayList<>();
      // Pick up the first one for an attribute
      if (attributes != null) {
        attributes.forEach(
            attribute -> {
              outAttributes.add(attribute);
              reducers.add(
                  (acc, event) ->
                      acc != null
                          ? acc
                          : event.get(cassStream.getAttributeDesc(attribute).getName()));
            });
      } else {
        cassStream
            .getAttributeTable()
            .forEach(
                (name, desc) -> {
                  outAttributes.add(desc.getName());
                  reducers.add((acc, event) -> acc != null ? acc : event.get(desc.getName()));
                });
      }

      if (aggregates != null) {
        aggregates.forEach(
            aggregate -> {
              outAttributes.add(aggregate.getOutputAttributeName());
              reducers.add(createReducer(aggregate, cassStream));
            });
      }

      dimensions = new ArrayList<>();
      if (view.getDimensions() != null) {
        for (String dimension : view.getDimensions()) {
          dimensions.add(cassStream.getAttributeDesc(dimension).getName());
        }
      } else {
        dimensions.add(cassStream.getAttributeDesc(view.getBy()).getName());
      }

      this.eventSupplier = eventSupplier;
    }

    @Getter
    public static class Context {
      private final Map<List<Object>, Object[]> groups = new LinkedHashMap<>();
    }

    public Context start() {
      return new Context();
    }

    public void consumeRecord(Event event, Context ctx) {
      final var groups = ctx.getGroups();
      List<Object> groupKeys = new ArrayList<>(dimensions.size());
      for (String dimension : dimensions) {
        groupKeys.add(event.get(dimension));
      }
      Object[] values = groups.get(groupKeys);
      if (values == null) {
        values = new Object[outAttributes.size()];
        groups.put(groupKeys, values);
      }
      for (int i = 0; i < outAttributes.size(); ++i) {
        values[i] = reducers.get(i).consume(values[i], event);
      }
    }

    public List<Event> finish(Context ctx) {
      final var groups = ctx.getGroups();
      // convert groups to events
      final List<Event> output = new ArrayList<>(groups.size());
      groups.forEach(
          (groupKeys, values) -> {
            final var event = eventSupplier.get();
            for (int i = 0; i < dimensions.size(); ++i) {
              event.set(dimensions.get(i), groupKeys.get(i));
            }
            for (int i = 0; i < outAttributes.size(); ++i) {
              event.set(outAttributes.get(i), reducers.get(i).finish(values[i]));
            }
            output.add(event);
          });

      return output;
    }
  }

  /**
   * Generates aggregate function nextCurrentValue = func(currentValue, newEvent).
   *
   * @param aggregate Aggregate function descriptor
   * @param cassStream Stream info
   * @return Aggregate function
   */
  private static SignalReducer createReducer(Aggregate aggregate, CassStream cassStream) {
    if (aggregate != null) {
      switch (aggregate.getFunction()) {
        case SUM:
          {
            AttributeDesc valueAttr = cassStream.getAttributeDesc(aggregate.getBy());
            InternalAttributeType type = valueAttr.getAttributeType();
            return (currentValue, newEvent) -> {
              final var newValue = newEvent.get(valueAttr.getName());
              if (currentValue == null) {
                return newValue;
              } else {
                return type.add(currentValue, newValue);
              }
            };
          }
        case COUNT:
          return (acc, newEvent) -> acc != null ? ((Long) acc) + 1 : Long.valueOf(1);
        case MIN:
          {
            final AttributeDesc valueAttr = cassStream.getAttributeDesc(aggregate.getBy());
            final BiFunction<Object, Object, Object> min =
                Comparators.generateMinFunction(valueAttr.getAttributeType(), false);
            return (currentValue, newEvent) -> {
              final var newValue = newEvent.get(valueAttr.getName());
              return min.apply(currentValue, newValue);
            };
          }
        case MAX:
          {
            final AttributeDesc valueAttr = cassStream.getAttributeDesc(aggregate.getBy());
            final BiFunction<Object, Object, Object> max =
                Comparators.generateMaxFunction(valueAttr.getAttributeType(), false);
            return (currentValue, newEvent) -> {
              final var newValue = newEvent.get(valueAttr.getName());
              return max.apply(currentValue, newValue);
            };
          }
        case LAST:
          {
            final AttributeDesc valueAttr = cassStream.getAttributeDesc(aggregate.getBy());
            return (currentValue, newEvent) -> {
              final var newValue = newEvent.get(valueAttr.getName());
              return newValue;
            };
          }
        case DISTINCTCOUNT:
          return new SignalReducer() {
            private final AttributeDesc valueAttr = cassStream.getAttributeDesc(aggregate.getBy());

            @Override
            public Object consume(Object acc, Event event) {
              if (acc == null) {
                acc = new HashSet<>();
              }
              ((Set) acc).add(event.get(valueAttr.getName()));
              return acc;
            }

            @Override
            public Object finish(Object acc) {
              return acc == null ? 0 : Long.valueOf(((Set) acc).size());
            }
          };
        case AVG:
          {
            // resolve attribute names
            final var valueAttrName = cassStream.getAttributeDesc(aggregate.getBy()).getName();
            final String countAttrName;
            if (aggregate instanceof Avg) {
              final var avg = (Avg) aggregate;
              countAttrName =
                  avg.countAttribute() != null
                      ? cassStream.getAttributeDesc(avg.countAttribute()).getName()
                      : null;
            } else {
              countAttrName = null;
            }

            return new SignalReducer() {
              private final Function<Event, Number> valueRetriever =
                  (event) -> (Number) event.get(valueAttrName);

              private final Function<Event, Long> countRetriever =
                  countAttrName != null
                      ? (event) -> (Long) event.get(countAttrName)
                      : (event) -> 1L;

              @Override
              public Object consume(Object acc, Event event) {
                if (acc == null) {
                  acc = new AverageAccumulator(valueRetriever, countRetriever);
                }
                ((AverageAccumulator) acc).accumulate(event);
                return acc;
              }

              @Override
              public Object finish(Object acc) {
                return acc == null ? 0 : ((AverageAccumulator) acc).finish();
              }
            };
          }
        default:
          logger.error("Unknown aggregate={}", aggregate.getFunction());
      }
    }
    throw new UnsupportedOperationException("not implemented");
  }

  private static class AverageAccumulator {
    private final Function<Event, Number> valueRetriever;
    private final Function<Event, Long> countRetriever;
    private double sum;
    private long count;

    public AverageAccumulator(
        Function<Event, Number> valueRetriever, Function<Event, Long> countRetriever) {
      this.valueRetriever = valueRetriever;
      this.countRetriever = countRetriever;
      sum = 0;
      count = 0;
    }

    public void accumulate(Event event) {
      final var value = valueRetriever.apply(event);
      final var incrementBy = countRetriever.apply(event);
      if (value != null && incrementBy != null) {
        sum += value.doubleValue();
        count += incrementBy;
      }
    }

    public Double finish() {
      return count == 0 ? 0 : sum / count;
    }
  }

  /**
   * Method that validates view configuration.
   *
   * @param view View configuration.
   * @param cassStream Cassandra stream info to be used for validation.
   * @return The method returns null when the given view is valid. Error message otherwise.
   */
  public static String validateView(View view, CassStream cassStream) {
    if (view == null) {
      return null;
    }
    final ViewFunction viewFunction = view.getFunction();
    if (viewFunction == null) {
      return "View function must be set";
    }
    final String by = view.getBy();
    final List<String> dimensions = view.getDimensions();
    if (by == null && dimensions == null) {
      return "View by attribute or dimensions must be set";
    }
    if (dimensions != null) {
      for (String dimension : dimensions) {
        final String error = validateDimension(viewFunction, dimension, cassStream);
        if (error != null) {
          return error;
        }
      }
    } else {
      final String error = validateDimension(viewFunction, by, cassStream);
      if (error != null) {
        return error;
      }
    }
    return null;
  }

  private static String validateDimension(
      ViewFunction viewFunction, String dimension, CassStream cassStream) {
    final AttributeDesc viewKey = cassStream.getAttributeDesc(dimension);
    if (viewKey == null) {
      return dimension + ": view key not found in stream attributes";
    }
    if (viewFunction == ViewFunction.SORT && !viewKey.getAttributeType().isComparable()) {
      return String.format(
          "View key attribute %s (%s) must be comparable for SORT view function",
          dimension, viewKey.getAttributeType().name());
    }
    return null;
  }

  /**
   * This method validates specified aggregate objects.
   *
   * <p>If any invalid aggregate objects exist, the method breaks at the first invalid object and
   * returns the failure reason of this one.
   *
   * @param aggregates Aggregate objects to validate
   * @param cassStream CassStream object that corresponds to the specified aggregates.
   * @return Null if all aggregate objects are valid, otherwise returns the failure reason of the
   *     first invalid object.
   * @throws TfosException When an user error happens.
   */
  public static String validateAggregates(
      List<? extends Aggregate> aggregates, CassStream cassStream, Set<String> eventAttributeKeys)
      throws TfosException {
    if (aggregates == null) {
      return null;
    }
    for (int i = 0; i < aggregates.size(); ++i) {
      final Aggregate aggregate = aggregates.get(i);
      if (aggregate == null) {
        throw new InvalidValueException(
            String.format("aggregates[%d]: aggregate entry may not be null", i));
      }
      if (aggregate.getFunction() != MetricFunction.COUNT) {
        final CassAttributeDesc desc = cassStream.getAttributeDesc(aggregate.getBy());
        if (desc == null) {
          throw new TfosException(EventExtractError.INVALID_ATTRIBUTE, aggregate.getBy());
        }
        ((MutableAggregate) aggregate).setBy(desc.getName());
        if (aggregate.getFunction() == MetricFunction.SUM) {
          if (!desc.getAttributeType().isAddable()) {
            throw new ConstraintViolationException(
                String.format(
                    "Attribute '%s' of type %s for SUM aggregate is not addable",
                    aggregate.getBy(), desc.getAttributeType().name()));
          }
        } else {
          if (!desc.getAttributeType().isComparable()) {
            throw new ConstraintViolationException(
                String.format(
                    "Attribute '%s' of type %s for %s aggregate is not comparable",
                    aggregate.getBy(),
                    desc.getAttributeType().name(),
                    aggregate.getFunction().name()));
          }
        }
      }
      final String key = aggregate.getOutputAttributeName();
      if (eventAttributeKeys.contains(key)) {
        throw new ConstraintViolationException(
            String.format(
                "aggregates[%d]: duplicate event attribute key; aggregate=%s", i, aggregate));
      }
      if (cassStream.getAttributeDesc(key) != null) {
        throw new ConstraintViolationException(
            String.format(
                "aggregates[%d]: event attribute key conflicts with an existing attribute;"
                    + " aggregate=%s",
                i, aggregate));
      }
      eventAttributeKeys.add(key);
    }
    return null;
  }

  public static void validateFilter(String filter, QueryExecutionState state, CassStream cassStream)
      throws TfosException, ApplicationException {
    // validate filter
    if (filter != null && !filter.isEmpty()) {
      state.setFilter(
          cassStream.parseFilter(filter).stream()
              .map((relation) -> relation)
              .collect(Collectors.toList()));
      cassStream.validateFilter(state.getFilter());
    }
  }
}
