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
package io.isima.bios.admin.v1;

import io.isima.bios.models.EnrichmentConfigContext;
import io.isima.bios.models.IngestTimeLagEnrichmentConfig;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class StreamComparators {
  /**
   * Utility method to check if two StreamConfig objects are identical.
   *
   * <p>This method is meant to be used for checking if an input object changes existing one. So the
   * method ignores parent, additionalEventAttributes, preprocessStages, deleted flag, and version.
   *
   * @param left Left parameter
   * @param right Right parameter
   * @return true if two parameters are identical, false otherwise.
   */
  public static boolean equals(StreamConfig left, StreamConfig right) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);
    if (left == right) {
      return true;
    }
    if (!pointerConsistencyCheck(left.getName(), right.getName())
        || (left.getName() != null && !left.getName().equalsIgnoreCase(right.getName()))
        || (left.getType() != right.getType())
        || !pointerConsistencyCheck(left.getDescription(), right.getDescription())
        || (left.getDescription() != null && !left.getDescription().equals(right.getDescription()))
        || !pointerConsistencyCheck(left.getAttributes(), right.getAttributes())) {
      return false;
    }
    int numAttr = left.getAttributes().size();
    if (numAttr != right.getAttributes().size()) {
      return false;
    }
    if (numAttr == 0
        && (!pointerConsistencyCheck(left.getMissingValuePolicy(), right.getMissingValuePolicy())
            || left.getMissingValuePolicy() != right.getMissingValuePolicy())) {
      return false;
    }
    for (int i = 0; i < numAttr; ++i) {
      if (!equals(
          left.getAttributes().get(i),
          left.getMissingValuePolicy(),
          right.getAttributes().get(i),
          right.getMissingValuePolicy())) {
        return false;
      }
    }
    if (left.getType() == StreamType.CONTEXT) {
      var leftPrimaryKey = left.getPrimaryKey();
      if (leftPrimaryKey == null
          && left.getAttributes() != null
          && !left.getAttributes().isEmpty()) {
        leftPrimaryKey = List.of(left.getAttributes().get(0).getName());
      }
      var rightPrimaryKey = right.getPrimaryKey();
      if (rightPrimaryKey == null
          && right.getAttributes() != null
          && !right.getAttributes().isEmpty()) {
        rightPrimaryKey = List.of(right.getAttributes().get(0).getName());
      }
      if (!Objects.equals(leftPrimaryKey, rightPrimaryKey)) {
        return false;
      }
    }
    final List<PreprocessDesc> lpp = left.getPreprocesses();
    final List<PreprocessDesc> rpp = right.getPreprocesses();
    if (!pointerConsistencyCheck(lpp, rpp)) {
      return false;
    }
    if (lpp != null) {
      int npp = lpp.size();
      if (npp != rpp.size()) {
        return false;
      }
      for (int i = 0; i < npp; ++i) {
        if (!equals(lpp.get(i), rpp.get(i))) {
          return false;
        }
      }
    }
    final List<IngestTimeLagEnrichmentConfig> ldyn = left.getIngestTimeLag();
    final List<IngestTimeLagEnrichmentConfig> rdyn = right.getIngestTimeLag();
    if (!pointerConsistencyCheck(ldyn, rdyn)) {
      return false;
    }
    if (ldyn != null && !equals(ldyn, rdyn)) {
      return false;
    }
    final List<ViewDesc> lviews = left.getViews();
    final List<ViewDesc> rviews = right.getViews();
    if (!pointerConsistencyCheck(lviews, rviews)) {
      return false;
    }
    if (lviews != null) {
      int nviews = lviews.size();
      if (nviews != rviews.size()) {
        return false;
      }
      final var rviewsMap = new HashMap<String, ViewDesc>();
      rviews.forEach((view) -> rviewsMap.put(view.getName().toLowerCase(), view));
      for (int i = 0; i < nviews; ++i) {
        final var lview = lviews.get(i);
        final var rview = rviewsMap.get(lview.getName().toLowerCase());
        if (rview == null || !equals(lview, rview)) {
          return false;
        }
      }
    }

    final List<PostprocessDesc> lpops = left.getPostprocesses();
    final List<PostprocessDesc> rpops = right.getPostprocesses();
    if (!pointerConsistencyCheck(lpops, rpops)) {
      return false;
    }
    if (lpops != null) {
      int npop = lpops.size();
      if (npop != rpops.size()) {
        return false;
      }
      final var rpopsMap = new HashMap<String, PostprocessDesc>();
      rpops.forEach((pop) -> rpopsMap.put(pop.getView().toLowerCase(), pop));
      for (int i = 0; i < npop; ++i) {
        final var lpop = lpops.get(i);
        final var rpop = rpopsMap.get(lpop.getView().toLowerCase());
        if (rpop == null || !equals(lpop, rpop)) {
          return false;
        }
      }
    }

    final var lmlp =
        left.getMissingLookupPolicy() != null
            ? left.getMissingLookupPolicy()
            : MissingAttributePolicyV1.FAIL_PARENT_LOOKUP;
    final var rmlp =
        right.getMissingLookupPolicy() != null
            ? right.getMissingLookupPolicy()
            : MissingAttributePolicyV1.FAIL_PARENT_LOOKUP;
    if (lmlp != rmlp) {
      return false;
    }

    final var ldec = left.getExportDestinationId() != null ? left.getExportDestinationId() : "";
    final var rdec = right.getExportDestinationId() != null ? right.getExportDestinationId() : "";
    if (!ldec.equals(rdec)) {
      return false;
    }

    final List<EnrichmentConfigContext> lce = left.getContextEnrichments();
    final List<EnrichmentConfigContext> rce = right.getContextEnrichments();
    if (!pointerConsistencyCheck(lce, rce)) {
      return false;
    }
    if ((lce != null) && !lce.equals(rce)) {
      return false;
    }

    final var lauditEnabled = left.getAuditEnabled();
    final var rauditEnabled = right.getAuditEnabled();
    if (!Objects.equals(lauditEnabled, rauditEnabled)) {
      return false;
    }

    if (!Objects.equals(left.getTtl(), right.getTtl())) {
      return false;
    }

    final List<FeatureDesc> lf = left.getFeatures();
    final List<FeatureDesc> rf = right.getFeatures();
    if (!pointerConsistencyCheck(lf, rf)) {
      return false;
    }
    if ((lf != null) && !lf.equals(rf)) {
      return false;
    }

    return true;
  }

  private static boolean equals(
      List<IngestTimeLagEnrichmentConfig> ldyn, List<IngestTimeLagEnrichmentConfig> rdyn) {
    if (ldyn.size() != rdyn.size()) {
      return false;
    }
    for (int i = 0; i < ldyn.size(); ++i) {
      final var left = ldyn.get(i);
      final var right = rdyn.get(i);
      if (left == null && right != null || left != null && right == null) {
        return false;
      }
      if (left != null) {
        if (left.getName().equalsIgnoreCase(right.getName())
            || !left.getAttribute().equalsIgnoreCase(right.getAttribute())
            || Objects.equals(left.getAs(), right.getAs())
            || Objects.equals(left.getTags(), right.getTags())
            || Objects.equals(left.getFillIn(), right.getFillIn())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Utility method to check if two AttributeDesc objects are identical.
   *
   * @param left Left parameter
   * @param right Right parameter
   * @return true if two parameters are identical, false otherwise.
   */
  public static boolean equals(
      AttributeDesc left,
      MissingAttributePolicyV1 leftMasterPolicy,
      AttributeDesc right,
      MissingAttributePolicyV1 rightMasterPolicy) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);
    if (!left.getName().equalsIgnoreCase(right.getName())
        || left.getAttributeType() != right.getAttributeType()) {
      return false;
    }

    if (!Objects.equals(left.getEnum(), right.getEnum())) {
      return false;
    }

    final MissingAttributePolicyV1 leftPolicy =
        left.getMissingValuePolicy() != null ? left.getMissingValuePolicy() : leftMasterPolicy;
    final MissingAttributePolicyV1 rightPolicy =
        right.getMissingValuePolicy() != null ? right.getMissingValuePolicy() : rightMasterPolicy;
    if (leftPolicy != rightPolicy) {
      return false;
    }
    if (!Objects.equals(left.getTags(), right.getTags())) {
      return false;
    }
    return pointerConsistencyCheck(left.getDefaultValue(), right.getDefaultValue())
        && (left.getDefaultValue() == null
            || left.getDefaultValue().equals(right.getDefaultValue()));
  }

  /**
   * Utility method to check if two PreprocessDesc objects are identical.
   *
   * <p>This method is meant to be used for checking if an input object changes existing one. So the
   * method ignores version.
   *
   * @param left Left parameter
   * @param right Right parameter
   * @return true if two parameters are identical, false otherwise.
   */
  public static boolean equals(PreprocessDesc left, PreprocessDesc right) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);
    if (left == right) {
      return true;
    }
    if (!pointerConsistencyCheck(left.getName(), right.getName())
        || (left.getName() != null && !left.getName().equalsIgnoreCase(right.getName()))) {
      return false;
    }

    final List<String> leftForeignKey =
        left.getForeignKey() != null
            ? left.getForeignKey()
            : (left.getCondition() != null ? List.of(left.getCondition()) : null);
    final List<String> rightForeignKey =
        right.getForeignKey() != null
            ? right.getForeignKey()
            : (right.getCondition() != null ? List.of(right.getCondition()) : null);
    if (!compareStringList(leftForeignKey, rightForeignKey)) {
      return false;
    }

    if (!Objects.equals(left.getMissingLookupPolicy(), right.getMissingLookupPolicy())) {
      return false;
    }
    List<ActionDesc> la = left.getActions();
    List<ActionDesc> ra = right.getActions();
    if (!pointerConsistencyCheck(la, ra)) {
      return false;
    }
    if (la != null) {
      if (la.size() != ra.size()) {
        return false;
      }
      for (int i = 0; i < la.size(); ++i) {
        if (!equals(la.get(i), ra.get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  private static boolean compareStringList(List<String> left, List<String> right) {
    if (!pointerConsistencyCheck(left, right)) {
      return false;
    }
    if (left == null) {
      return true;
    }
    if (left.size() != right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); ++i) {
      final var lstring = left.get(i);
      final var rstring = right.get(i);
      if (!pointerConsistencyCheck(lstring, rstring)) {
        return false;
      }
      if (lstring != null && !lstring.equalsIgnoreCase(rstring)) {
        return false;
      }
    }
    return true;
  }

  /** Utility method to check if two ViewDesc objects are identical, ignoring version. */
  public static boolean equals(ViewDesc left, ViewDesc right) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);
    if (left == right) {
      return true;
    }
    if (!(left.getName() == null
        ? right.getName() == null
        : left.getName().equalsIgnoreCase(right.getName()))) {
      return false;
    }
    if (!compareStringList(left.getGroupBy(), right.getGroupBy())) {
      return false;
    }
    if (!compareStringList(left.getAttributes(), right.getAttributes())) {
      return false;
    }
    if (!(left.getDataSketches() == null
        ? right.getDataSketches() == null
        : left.getDataSketches().equals(right.getDataSketches()))) {
      return false;
    }
    final var lTableEnabled = Objects.requireNonNullElse(left.getIndexTableEnabled(), false);
    final var rTableEnabled = Objects.requireNonNullElse(right.getIndexTableEnabled(), false);
    if (!Objects.equals(lTableEnabled, rTableEnabled)) {
      return false;
    }
    if (!Objects.equals(left.getIndexType(), right.getIndexType())) {
      return false;
    }
    if (!Objects.equals(left.getTimeIndexInterval(), right.getTimeIndexInterval())) {
      return false;
    }
    final var lSnapshot = Objects.requireNonNullElse(left.getSnapshot(), false);
    final var rSnapshot = Objects.requireNonNullElse(right.getSnapshot(), false);
    if (!Objects.equals(lSnapshot, rSnapshot)) {
      return false;
    }
    final var lIndex = Objects.requireNonNullElse(left.getWriteTimeIndexing(), false);
    final var rIndex = Objects.requireNonNullElse(right.getWriteTimeIndexing(), false);
    if (!Objects.equals(lIndex, rIndex)) {
      return false;
    }
    return Objects.equals(left.getFeatureAsContextName(), right.getFeatureAsContextName())
        && Objects.equals(left.getLastNItems(), right.getLastNItems())
        && Objects.equals(left.getLastNTtl(), right.getLastNTtl());
  }

  /** Utility method to check if two PostprocessDesc objects are identical, ignoring version. */
  public static boolean equals(PostprocessDesc left, PostprocessDesc right) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);
    if (left == right) {
      return true;
    }
    if (!(left.getView() == null
        ? right.getView() == null
        : left.getView().equalsIgnoreCase(right.getView()))) {
      return false;
    }
    final List<Rollup> lRollups = left.getRollups();
    final List<Rollup> rRollups = right.getRollups();
    if (!pointerConsistencyCheck(lRollups, rRollups)) {
      return false;
    }
    if (lRollups != null) {
      int numRollups = lRollups.size();
      if (numRollups != rRollups.size()) {
        return false;
      }
      for (int i = 0; i < numRollups; ++i) {
        if (!equals(lRollups.get(i), rRollups.get(i))) {
          return false;
        }
      }
    }

    return true;
  }

  /** Utility method to check if two Rollup objects are identical, ignoring version. */
  public static boolean equals(Rollup left, Rollup right) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);
    if (left == right) {
      return true;
    }
    if (!(left.getName() == null
        ? right.getName() == null
        : left.getName().equalsIgnoreCase(right.getName()))) {
      return false;
    }
    if (!(left.getInterval() == null
        ? right.getInterval() == null
        : left.getInterval().equals(right.getInterval()))) {
      return false;
    }
    if (!(left.getHorizon() == null
        ? right.getHorizon() == null
        : left.getHorizon().equals(right.getHorizon()))) {
      return false;
    }
    if (!(left.getAlerts() == null
        ? right.getAlerts() == null
        : left.getAlerts().equals(right.getAlerts()))) {
      return false;
    }
    if (left.getSchemaName() != null
        && right.getSchemaName() != null
        && !left.getSchemaName().equalsIgnoreCase(right.getSchemaName())) {
      return false;
    }
    if (left.getSchemaVersion() != null
        && right.getSchemaVersion() != null
        && !left.getSchemaVersion().equals(right.getSchemaVersion())) {
      return false;
    }
    return true;
  }

  /**
   * Utility method to check if two ActionDesc objects are identical.
   *
   * @param left Left parameter
   * @param right Right parameter
   * @return true if two parameters are identical, false otherwise.
   */
  public static boolean equals(ActionDesc left, ActionDesc right) {
    Objects.requireNonNull(left);
    Objects.requireNonNull(right);
    if (left == right) {
      return true;
    }
    if (!pointerConsistencyCheck(left.getActionType(), right.getActionType())
        || (left.getActionType() != null && left.getActionType() != right.getActionType())
        || !pointerConsistencyCheck(left.getAttribute(), right.getAttribute())
        || (left.getAttribute() != null
            && !left.getAttribute().equalsIgnoreCase(right.getAttribute()))
        || !pointerConsistencyCheck(left.getContext(), right.getContext())
        || (left.getContext() != null && !left.getContext().equalsIgnoreCase(right.getContext())
            || !pointerConsistencyCheck(left.getDefaultValue(), right.getDefaultValue())
            || left.getDefaultValue() != null
                && !left.getDefaultValue().equals(right.getDefaultValue())
            || !pointerConsistencyCheck(left.getAs(), right.getAs())
            || left.getAs() != null && !left.getAs().equalsIgnoreCase(right.getAs()))) {
      return false;
    }
    return true;
  }

  private static <T> boolean pointerConsistencyCheck(T left, T right) {
    return (left == null && right == null) || (left != null && right != null);
  }
}
