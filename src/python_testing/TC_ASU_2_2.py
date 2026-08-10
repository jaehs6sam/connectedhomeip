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


class TC_ASU_2_2(MatterBaseTest):
    def desc_TC_ASU_2_2(self) -> str:
        return "[TC-ASU-2.2] Attributes with DUT as a server"

    def pics_TC_ASU_2_2(self):
        return ["ASU.S"]

    def steps_TC_ASU_2_2(self) -> list[TestStep]:
        return [
            TestStep("1", "Commissioning, already done", is_commissioning=True),
            TestStep("2", "TH establishes a wildcard subscription to all attributes on Ambient Sensing Union Cluster on the endpoint under test."),
            TestStep("3", "Add a new contributor(s) to _{A_UNIONCONTRIBUTORLIST}_ attribute."),
            TestStep("4", "TH verifies that the new contributor(s) added to the _{A_UNIONCONTRIBUTORLIST}_ attribute is same as the one from the step 3."),
            TestStep("5", "TH receives _{E_UNIONCONTRIBUTORADD}_ event and reads the AddedContributor field and verifies that the AddedContributor event field contains the same struct type data from the step 3."),
            TestStep("6", "Remove one of existing contributors from _{A_UNIONCONTRIBUTORLIST}_ attribute."),
            TestStep("7", "TH verifies that the removed contributor(s) from the step 6 is not included in the _{A_UNIONCONTRIBUTORLIST}_ attribute."),
            TestStep("8", "TH receives _{E_UNIONCONTRIBUTORREMOVE}_ event and reads the RemovedContributor field and Verifies that the RemovedContributor event field contains the same struct type data from the step 6."),
            TestStep("9", "Change the ContributorStatus value of one contributor from _{A_UNIONCONTRIBUTORLIST}_ attribute, and save the contributor's list index value and the ContributorStatus value before the change."),
            TestStep("10", "TH verifies that the ContributorStatus value of the contributor changed from the step 9 is updated accordingly."),
            TestStep("11", "TH receives _{E_UNIONCONTRIBUTORSTATUSCHANGE}_ event and verifies the ContributorStatusChange field values match to the field value changes occurred in the step 9. ") 
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

        self.step("3", "Add a new contributor(s) to _{A_UNIONCONTRIBUTORLIST}_ attribute.")
        # read UnionHealth attribute
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

        self.step("4", "TH verifies that the new contributor(s) added to the _{A_UNIONCONTRIBUTORLIST}_ attribute is same as the one from the step 3.")
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

        self.step("5", "TH receives _{E_UNIONCONTRIBUTORADD}_ event and reads the AddedContributor field and verifies that the AddedContributor event field contains the same struct type data from the step 3.")
        # Check if UnionContributorAdded event is detected
        event = event_listener.get_last_event()
        
        # check event field is sent correctly.
        asserts.assert_equal(
            event.Data.addedContributor.contributorNodeID, contnode, "Wrong Contributor Node ID")
        asserts.assert_equal(event.Data.addedContributor.contributorEndpointID, contend, "Wrong Contributor Endpoint ID")
        asserts.assert_equal(event.Data.addedContributor.contributorName, contname, "Wrong Contributor Name")
        asserts.assert_equal(event.Data.addedContributor.contributorHealth, conthealth, "Wrong Contributor Health Status")

        event_listener.reset()

        self.step("6", "Remove one of existing contributors from _{A_UNIONCONTRIBUTORLIST}_ attribute.")

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
        
        self.step("7", "TH verifies that the removed contributor(s) from the step 6 is not included in the _{A_UNIONCONTRIBUTORLIST}_ attribute.")
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

        self.step("8", "TH receives _{E_UNIONCONTRIBUTORREMOVE}_ event and reads the RemovedContributor field and Verifies that the RemovedContributor event field contains the same struct type data from the step 6.")
        # Check if UnionContributorRemoved event is detected
        event = event_listener.get_last_event()
        
        # check event field is sent correctly.
        asserts.assert_equal(
            event.Data.removedContributor.contributorNodeID, contnode, "Wrong Contributor Node ID")
        asserts.assert_equal(event.Data.removedContributor.contributorEndpointID, contend, "Wrong Contributor Endpoint ID")
        asserts.assert_equal(event.Data.removedContributor.contributorName, contname, "Wrong Contributor Name")
        asserts.assert_equal(event.Data.removedContributor.contributorHealth, conthealth, "Wrong Contributor Health Status")

        event_listener.reset()

        self.step("9", "Change the ContributorStatus value of one contributor from _{A_UNIONCONTRIBUTORLIST}_ attribute, and save the contributor's list index value and the ContributorStatus value before the change.")

        # ci interaction
        if self.is_ci:
            contindex = 1
            prev_status = 1
            current_status = 0
            self.write_to_app_pipe(
                f'{{"Name":"ChangeUnionContributorStatus", "EndpointId":{endpoint}, "UnionContributorList":[{{"ContributorIndex":{contindex}, "ContributorStatus":{current_status}}}]}}')
            # Add 1 second delay to make sure it's done
            await asyncio.sleep(1)
        else:
            self.wait_for_user_input(
                prompt_msg="Type any letter and press ENTER after changing a contributor's status in UnionContributorList.")
        
        self.step("10", "TH verifies that the ContributorStatus value of the contributor changed from the step 9 is updated accordingly.")
        # subscription check
        subscription_expected = attrib_listener.attribute_reports[cluster.Attributes.UnionContributorList][0].value

        contributor = subscription_expected[contindex]
        asserts.assert_equal(contributor.contributorHealth, current_status, "ContributorHealth is expected to be changed as the one changed in step 9.")

        attrib_listener.reset()

        self.step("11", "TH receives _{E_UNIONCONTRIBUTORSTATUSCHANGE}_ event and verifies the ContributorStatusChange field values match to the field value changes occurred in the step 9.")
        # Check if UnionContributorStatusChanged event is detected
        event = event_listener.get_last_event()
        
        # check event field is sent correctly.
        asserts.assert_equal(
            event.Data.contributorStatusChanged.contributorIndex, contindex, "Wrong Contributor Index")
        asserts.assert_equal(event.Data.contributorStatusChanged.previousContributorStatus, prev_status, "Wrong Previous Contributor Status")
        asserts.assert_equal(event.Data.contributorStatusChanged.currentContributorStatus, current_status, "Wrong Current Contributor Status")

        event_listener.reset()

if __name__ == "__main__":
    default_matter_test_main()
