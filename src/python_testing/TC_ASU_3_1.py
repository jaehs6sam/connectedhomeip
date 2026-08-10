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

# Script Function Call Example
# ./scripts/tests/run_python_test.py --app out/linux-x64-all-clusters/chip-all-clusters-app --factory-reset
# --app-args "--KVS kvs1 --discriminator 1234" --script src/python_testing/TC_ASU_2_1.py
# --script-args "--storage-path admin_storage1.json --discriminator 1234 --passcode 20202021 --commissioning-method on-network --endpoint 1"


class TC_ASU_3_1(MatterBaseTest):
    def desc_TC_ASU_3_1(self) -> str:
        return "[TC-ASU-3.1] Attributes with DUT as a server"

    def pics_TC_ASU_3_1(self):
        return ["ASU.S"]

    def steps_TC_ASU_3_1(self) -> list[TestStep]:
        return [
            TestStep("1", "Commissioning, already done", is_commissioning=True),
            TestStep("2", "TH establishes a wildcard subscription to all attributes on Ambient Sensing Union Cluster on the endpoint under test."),
            TestStep("3", "TH changes UnionName attribute."),
            TestStep("4", "TH verifies that the value of UnionName attribute reflects the change made in the step 3."),
            TestStep("5", "Change UnionHealth attribute."),
            TestStep("6", "TH verifies that  the value of UnionHealth attribute reflects the change made in the step 5."),
            TestStep("7", "Change UnionContributorList attribute by adding a contributor from the UnionContributorList."),
            TestStep("8", "TH verifies that the value of UnionContributorList attribute reflects the contributor added in the step 7."),
            TestStep("9", "Change UnionContributorList attribute by removing a contributor from the UnionContributorList."),
            TestStep("10", "TH verifies that the value of UnionContributorList attribute reflects the contributor removed in the step 9.")
        ]

    def setup_test(self):
        super().setup_test()
        self.is_ci = self.matter_test_config.global_test_params.get('simulate_ambientsensing', True)

    @run_if_endpoint_matches(has_cluster(Clusters.AmbientSensingUnion))
    async def test_TC_ASU_3_1(self):
        endpoint = self.get_endpoint()
        cluster = Clusters.AmbientSensingUnion
        attr = Clusters.AmbientSensingUnion.Attributes

        self.step("1", "Commissioning, already done", is_commissioning=True)
        # Commission DUT - already done

        self.step("2", "TH establishes a wildcard subscription to all attributes on Ambient Sensing Union Cluster on the endpoint under test with minIntervalFloor set to 0, MaxIntervalCeiling set to 30 and KeepSubscriptions set to false.")
        # subscription setup
        attrib_listener = AttributeSubscriptionHandler(expected_cluster=cluster)
        await attrib_listener.start(dev_ctrl, node_id, endpoint=endpoint, min_interval_sec=0, max_interval_sec=30, keepSubscriptions=False)

        # start event listener
        event_listener = EventSubscriptionHandler(expected_cluster=cluster)
        await event_listener.start(dev_ctrl, node_id, endpoint=endpoint, min_interval_sec=0, max_interval_sec=30)

        self.step("3", "TH changes UnionName attribute.")
        unionName_write ="TestUnionName"

        # write UnionName 
        await self.write_single_attribute(attr.UnionName(unionName_write))

        self.step("4", "TH verifies that the value of UnionName attribute reflects the change made in the step 3.")
        # subscription check
        subscription_expected = attrib_listener.attribute_reports[cluster.Attributes.UnionName]
        unionName_sub = subscription_expected[0].value
        asserts.assert_equal(unionName_sub, unionName_write, "UnionName attribute subscription is expected to be same as the written one.")

        attrib_listener.reset()

        self.step("5", "Change UnionHealth attribute.")
        # read UnionHealth attribute
        unionhealth_prev = await self.read_single_attribute_check_success(endpoint=endpoint, cluster=cluster, attribute=attr.UnionHealth)
        unionhealth_new = (unionhealth_prev + 1) % 3  # Change to a different value between 0 and 2 

        # ci interaction
        if self.is_ci:
            self.write_to_app_pipe(
                f'{{"Name":"ChangeUnionHealth", "EndpointId":{endpoint}, "UnionHealth":{unionhealth_new}}}')
            # Add 1 second delay to make sure it's done
            await asyncio.sleep(1)
        else:
            self.wait_for_user_input(
                prompt_msg="Type any letter and press ENTER after changing the UnionHealth attribute.")

        self.step("6", "TH verifies that the value of UnionHealth attribute reflects the change made in the step 5.")
        # subscription check
        subscription_expected = attrib_listener.attribute_reports[cluster.Attributes.UnionHealth][0].value
        asserts.assert_equal(subscription_expected, unionhealth_new, "UnionHealth attribute subscription is expected to be same as the written one.")

        self.step("7", "Change UnionContributorList attribute by adding a contributor from the UnionContributorList.")
        # read UnionContributorList attribute
        unionlist_prev = await self.read_single_attribute_check_success(endpoint=endpoint, cluster=cluster, attribute=attr.UnionContributorList)

        # ci interaction
        if self.is_ci:
            contnode = 12345678901234567890
            contend = 1234
            contname = "TestContributor"
            conthealth = 1
            self.write_to_app_pipe(
                f'{{"Name":"AddUnionContributor", "EndpointId":{endpoint}, "UnionContributorList":[{{"ContributorNodeId":{contnode}, "ContributorEndpointId":{contend},"ContributorName":{contname},"ContributorHealth":{conthealth}}}]}}')
            # Add 1 second delay to make sure it's done
            await asyncio.sleep(1)
        else:
            self.wait_for_user_input(
                prompt_msg="Type any letter and press ENTER after adding a contributor to UnionContributorList.")

        self.step("8", "TH verifies that the value of UnionContributorList attribute reflects the contributor added in the step 7.")
        # subscription check
        subscription_expected = attrib_listener.attribute_reports[cluster.Attributes.UnionContributorList][0].value

        exist_flag = False
        for i in range(len(subscription_expected)):
            contributor = subscription_expected[i]
            if contributor.contributorNodeID == contnode:
                asserts.assert_equal(contributor.contributorEndpointID, contend, "ContributorEndpointID is expected to be same as the one added in step 3.")
                asserts.assert_equal(contributor.contributorName, contname, "ContributorName is expected to be same as the one added in step 3.")
                asserts.assert_equal(contributor.contributorHealth, conthealth, "ContributorHealth is expected to be same as the one added in step 3.")
                exist_flag = True

        asserts.assert_true(exist_flag, "The added contributor is not found in the UnionContributorList.")

        attrib_listener.reset()

        self.step("9", "Change UnionContributorList attribute by removing a contributor from the UnionContributorList.")
        # read UnionContributorList attribute
        unionlist_prev = await self.read_single_attribute_check_success(endpoint=endpoint, cluster=cluster, attribute=attr.UnionContributorList)

        # ci interaction
        if self.is_ci:
            contnode = 12345678901234567890
            contend = 1234
            contname = "TestContributor"
            conthealth = 1
            self.write_to_app_pipe(
                f'{{"Name":"RemoveUnionContributor", "EndpointId":{endpoint}, "UnionContributorList":[{{"ContributorNodeId":{contnode}, "ContributorEndpointId":{contend},"ContributorName":{contname},"ContributorHealth":{conthealth}}}]}}')
            # Add 1 second delay to make sure it's done
            await asyncio.sleep(1)
        else:
            self.wait_for_user_input(
                prompt_msg="Type any letter and press ENTER after removing a contributor to UnionContributorList.")
        
        self.step("10", "TH verifies that the value of UnionContributorList attribute reflects the contributor removed in the step 9.")
        # subscription check
        subscription_expected = attrib_listener.attribute_reports[cluster.Attributes.UnionContributorList][0].value

        exist_flag = True
        for i in range(len(subscription_expected)):
            contributor = subscription_expected[i]
            if contributor.contributorNodeID == contnode:
                asserts.assert_equal(contributor.contributorEndpointID, contend, "ContributorEndpointID is expected to be same as the one added in step 3.")
                asserts.assert_equal(contributor.contributorName, contname, "ContributorName is expected to be same as the one added in step 3.")
                asserts.assert_equal(contributor.contributorHealth, conthealth, "ContributorHealth is expected to be same as the one added in step 3.")
                exist_flag = False

        asserts.assert_true(exist_flag, "The removed contributor is still found in the UnionContributorList.")

        attrib_listener.reset()


if __name__ == "__main__":
    default_matter_test_main()
