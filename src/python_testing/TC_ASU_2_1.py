#
#    Copyright (c) 2026 Project CHIP Authors
#    All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# See https://github.com/project-chip/connectedhomeip/blob/master/docs/testing/python.md#defining-the-ci-test-arguments
# for details about the block below.
#
# === BEGIN CI TEST ARGUMENTS ===
# test-runner-runs:
#   run1:
#     app: ${ALL_CLUSTERS_APP}
#     app-args: --discriminator 1234 --KVS kvs1 --trace-to json:${TRACE_APP}.json --app-pipe /tmp/asu_fifo
#     script-args: >
#       --storage-path admin_storage.json
#       --commissioning-method on-network
#       --discriminator 1234
#       --passcode 20202021
#       --endpoint 1
#       --app-pipe /tmp/asu_fifo
#     factory-reset: true
#     quiet: true
# === END CI TEST ARGUMENTS ===

import logging

import numpy as np
from mobly import asserts

import matter.clusters as Clusters
from matter.clusters.Types import NullValue
from matter.testing.decorators import has_cluster, run_if_endpoint_matches
from matter.testing.matter_testing import MatterBaseTest
from matter.testing.runner import TestStep, default_matter_test_main

log = logging.getLogger(__name__)

min_value_uint8 = np.iinfo(np.uint8).min
max_value_uint8 = np.iinfo(np.uint8).max
min_value_uint16 = np.iinfo(np.uint16).min
max_value_uint16 = np.iinfo(np.uint16).max
min_value_uint64 = np.iinfo(np.uint64).min
max_value_uint64 = np.iinfo(np.uint64).max


# Script Function Call Example
# ./scripts/tests/run_python_test.py --app out/linux-x64-all-clusters/chip-all-clusters-app --factory-reset
# --app-args "--KVS kvs1 --discriminator 1234" --script src/python_testing/TC_ASU_2_1.py
# --script-args "--storage-path admin_storage1.json --discriminator 1234 --passcode 20202021 --commissioning-method on-network --endpoint 1"


class TC_ASU_2_1(MatterBaseTest):
    def desc_TC_ASU_2_1(self) -> str:
        return "[TC-ASU-2.1] Attributes with DUT as a server"

    def pics_TC_ASU_2_1(self):
        return ["ASU.S"]

    def steps_TC_ASU_2_1(self) -> list[TestStep]:
        return [
            TestStep("1", "Commissioning, already done", is_commissioning=True),
            TestStep("2", "TH reads the UnionName attribute.",
                     "DUT response contains string characters."),
            TestStep("3", "TH reads the UnionHealth attribute.",
                     "DUT response contains a UnionHealthEnum type data."),
            TestStep("4", "TH reads the UnionContributorList attribute.",
                     "DUT response contains UnionContributorStruct data containing ContributorNodeID, ContributorEndpointID, ContributorName, and ContributorHealth.")
        ]

    def setup_test(self):
        super().setup_test()
        self.is_ci = self.matter_test_config.global_test_params.get('simulate_ambientsensing', True)

    @run_if_endpoint_matches(has_cluster(Clusters.AmbientSensingUnion))
    async def test_TC_ASU_2_1(self):
        endpoint = self.get_endpoint()
        cluster = Clusters.AmbientSensingUnion
        attr = Clusters.AmbientSensingUnion.Attributes

        self.step("1", "Commissioning, already done", is_commissioning=True)
        # Commission DUT - already done

        self.step("2", "TH writes a union name to DUT, and TH reads and subscribes to the UnionName attribute properly.")
        unionName_write ="TestUnionName"

        # subscription setup
        attrib_listener = AttributeSubscriptionHandler(expected_cluster=cluster)
        await attrib_listener.start(dev_ctrl, node_id, endpoint=endpoint, min_interval_sec=0, max_interval_sec=30, keepSubscriptions=False)

        # write UnionName 
        await self.write_single_attribute(attr.UnionName(unionName_write))

        # read UnionName attribute
        unionName_read = await self.read_single_attribute_check_success(endpoint=endpoint, cluster=cluster, attribute=attr.UnionName)
        asserts.assert_less_equal(unionName_read, unionName_write, "UnionName attribute read is expected to be same as the written one.")

        # subscription check
        subscription_expected = attrib_listener.attribute_reports[cluster.Attributes.UnionName]
        unionName_sub = subscription_expected[0].value
        asserts.assert_equal(unionName_sub, unionName_write, "UnionName attribute subscription is expected to be same as the written one.")

        attrib_listener.reset()

        self.step("3", "TH reads UnionHealth attribute value.")
        # read UnionHealth attribute
        unionHealth_read = await self.read_single_attribute_check_success(endpoint=endpoint, cluster=cluster, attribute=attr.UnionHealth)

        # checkvalue between 0 and 2
        asserts.assert_greater_equal(0, unionHealth_read_prev, "UnionHealth attribute read is expected to be between 0 and 2.")
        asserts.assert_less_equal(unionHealth_read_prev, 2, "UnionHealth attribute read is expected to be between 0 and 2.")

        self.step("4", "TH reads the UnionContributorList attribute. And check UnionContributorStruct data containing ContributorNodeID, ContributorEndpointID, ContributorName, and ContributorHealth.")
        # read UnionHealth attribute
        unionlist_read = await self.read_single_attribute_check_success(endpoint=endpoint, cluster=cluster, attribute=attr.UnionContributorList)

        # Determine the size of the list
        list_size = len(unionlist_read)

        for i in range(list_size):
            contributor = unionlist_read[i]

            asserts.assert_true(hasattr(contributor, 'contributorNodeID'), "ContributorNodeID is missing in UnionContributorStruct.")
            asserts.assert_true(hasattr(contributor, 'contributorEndpointID'), "ContributorEndpointID is missing in UnionContributorStruct.")
            asserts.assert_true(hasattr(contributor, 'contributorName'), "ContributorName is missing in UnionContributorStruct.")
            asserts.assert_true(hasattr(contributor, 'contributorHealth'), "ContributorHealth is missing in UnionContributorStruct.")

            if contributor.ContributorNodeID == NullValue:
                asserts.assert_true(contributor.ContributorEndpointID == NullValue, "ContributorEndpointID should be NullValue when ContributorNodeID is NullValue.")
                asserts.assert_true(isinstance(contributor.ContributorName, str), "ContributorName should be non empty string when ContributorNodeID is NullValue.")
            
            else:
                # ContributorNodeID
                asserts.assert_greater_equal(min_value_uint64, contributor.ContributorNodeID, "ContributorNodeID is expected to be unsigned 64-bit integer.")
                asserts.assert_less_equal(contributor.ContributorNodeID, max_value_uint64, "ContributorNodeID is expected to be unsigned 64-bit integer.")

                # ContributorEndpointID
                asserts.assert_greater_equal(min_value_uint16, contributor.ContributorEndpointID, "ContributorEndpointID is expected to be unsigned 16-bit integer.")
                asserts.assert_less_equal(contributor.ContributorEndpointID, max_value_uint16, "ContributorEndpointID is expected to be unsigned 16-bit integer.")

            # ContributorHealth
            asserts.assert_greater_equal(min_value_uint8, contributor.ContributorHealth, "ContributorHealth is expected to be unsigned 8-bit integer.")
            asserts.assert_less_equal(contributor.ContributorHealth, max_value_uint8, "ContributorHealth is expected to be unsigned 8-bit integer.")


if __name__ == "__main__":
    default_matter_test_main()
