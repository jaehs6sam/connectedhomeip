#
#    Copyright (c) 2024 Project CHIP Authors
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
#
# === BEGIN CI TEST ARGUMENTS ===
# test-runner-runs: run1
# test-runner-run/run1/app: ${ALL_CLUSTERS_APP}
# test-runner-run/run1/factoryreset: True
# test-runner-run/run1/quiet: True
# test-runner-run/run1/app-args: --discriminator 1234 --KVS kvs1 --trace-to json:${TRACE_APP}.json
# test-runner-run/run1/script-args: --storage-path admin_storage.json --commissioning-method on-network --discriminator 1234 --passcode 20202021 --trace-to json:${TRACE_TEST_JSON}.json --trace-to perfetto:${TRACE_TEST_PERFETTO}.perfetto --endpoint 1 --bool-arg simulate_occupancy:true
# === END CI TEST ARGUMENTS ===
#  There are CI issues to be followed up for the test cases below that implements manually controlling sensor device for
#  the occupancy state ON/OFF change.
#  [TC-OCC-3.1] test procedure step 3, 4
#  [TC-OCC-3.2] test precedure step 3a, 3c

import logging
import time
from typing import Any, Optional

import chip.clusters as Clusters
from chip.interaction_model import Status
from matter_testing_support import EventChangeCallback, MatterBaseTest, TestStep, async_test_body, default_matter_test_main
from mobly import asserts


class TC_OCC_3_1(MatterBaseTest):
    async def read_occ_attribute_expect_success(self, attribute):
        cluster = Clusters.Objects.OccupancySensing
        endpoint = self.matter_test_config.endpoint
        return await self.read_single_attribute_check_success(endpoint=endpoint, cluster=cluster, attribute=attribute)

    async def write_hold_time(self, hold_time: Optional[Any]) -> Status:
        dev_ctrl = self.default_controller
        node_id = self.dut_node_id
        endpoint = self.matter_test_config.endpoint

        cluster = Clusters.OccupancySensing
        write_result = await dev_ctrl.WriteAttribute(node_id, [(endpoint, cluster.Attributes.HoldTime(hold_time))])
        return write_result[0].Status

    def desc_TC_OCC_3_1(self) -> str:
        return "[TC-OCC-3.1] Primary functionality with server as DUT"

    def steps_TC_OCC_3_1(self) -> list[TestStep]:
        steps = [
            TestStep(1, "Commission DUT to TH.", is_commissioning=True),
            TestStep(2, "Change DUT HoldTime attribute value to HoldTimeMin that is 10 sec. If HoldTime is not supported, then skip this test step."),
            TestStep(3, "Set DUT Occupancy attribute to Unoccupied state. Set up Occupancy attribute subscription and event callback."),
            TestStep(4, "Operate on DUT to change the occupancy status and start a timer if HoldTime is supported."),
            TestStep(5, "After a timer passed HoldTime, TH reads Occupancy attribute from DUT.")
        ]
        return steps

    def pics_TC_OCC_3_1(self) -> list[str]:
        pics = [
            "OCC.S",
        ]
        return pics

    # Sends and out-of-band command to the all-clusters-app
    def write_to_app_pipe(self, command):
        with open(self.app_pipe, "w") as app_pipe:
            app_pipe.write(command + "\n")
        # Delay for pipe command to be processed (otherwise tests are flaky)
        time.sleep(0.001)

    # CI app pipe id creation
    self.app_pipe = "/tmp/chip_all_clusters_fifo_"
    #self.is_ci = self.check_pics("PICS_SDK_CI_ONLY")
    self.is_ci = self.matter_test_config.global_test_params['simulate_occupancy']
    if self.is_ci:
        app_pid = self.matter_test_config.app_pid
        if app_pid == 0:
            asserts.fail("The --app-pid flag must be set when PICS_SDK_CI_ONLY is set.c")
        self.app_pipe = self.app_pipe + str(app_pid) 
    
    @async_test_body
    async def test_TC_OCC_3_1(self):
        hold_time = 10  # 10 seconds for occupancy state hold time

        self.step(1)  # Commissioning already done

        self.step(2)

        cluster = Clusters.OccupancySensing
        attributes = cluster.Attributes
        attribute_list = await self.read_occ_attribute_expect_success(attribute=attributes.AttributeList)

        has_hold_time = attributes.HoldTime.attribute_id in attribute_list

        if has_hold_time:
            # write HoldTimeLimits HoldtimeMin to be 10 sec.
            await self.write_single_attribute(cluster.Attributes.HoldTimeLimits.HoldTimeMin(hold_time))
            # write 10 as a HoldTime attribute
            #asynch write_hold_time(hold_time)
            await self.write_single_attribute(cluster.Attributes.HoldTime(hold_time))
            # read HoldTime to check
            holdtime_dut = await self.read_occ_attribute_expect_success(attribute=attributes.HoldTime)
            asserts.assert_equal(holdtime_dut, holdtime, "HoldTime is not written to HoldTimeMin")
        else:
            logging.info("No HoldTime attribute supports. Will test only occupancy attribute triggering functionality")

        self.step(3)
        # check if Occupancy attribute is 0
        occupancy_dut = await self.read_occ_attribute_expect_success(attribute=attributes.Occupancy)

        # if occupancy is on, here try to set sensor occupancy state to 0.
        if occupancy_dut == 1:
            # Don't trigger occupancy sensor to render occupancy attribute to 0
            if has_hold_time:
                time.sleep(hold_time + 2.0)  # add some extra 2 seconds to ensure hold time has passed.
            else:  # a user wait until a sensor specific time to change occupancy attribute to 0.  This is the case where the sensor doesn't support HoldTime.
                # CI call to trigger off
                if self.is_ci:
                    self.write_to_app_pipe('{"Name":"SetOccupancy", "EndpointId": 1, "Occupancy": 0}')
                else:
                    self.wait_for_user_input(
                        prompt_msg="Type any letter and press ENTER after the sensor occupancy is detection ready state (occupancy attribute = 0)")

        # check sensor occupancy state is 0 for the next test step
        occupancy_dut = await self.read_occ_attribute_expect_success(attribute=attributes.Occupancy)
        asserts.assert_equal(occupancy_dut, 0, "Occupancy attribute is still 1.")

        # setup Occupancy attribute subscription here
        endpoint_id = self.matter_test_config.endpoint
        node_id = self.dut_node_id
        dev_ctrl = self.default_controller
        attrib_listener = ClusterAttributeChangeAccumulator(Clusters.Objects.OccupancySensing)
        post_prompt_settle_delay_seconds = 30
        await attrib_listener.start(dev_ctrl, node_id, endpoint=endpoint_id, min_interval_sec=0, max_interval_sec=post_prompt_settle_delay_seconds)

        # setup event
        events_callback = EventChangeCallback(Clusters.OccupancySensing)
        await events_callback.start(self.default_controller, node_id, endpoint_id)

        self.step(4)
        # CI call to trigger on
        if self.is_ci:
            self.write_to_app_pipe('{"Name":"SetOccupancy", "EndpointId": 1, "Occupancy": 1}')
        else:
            # Trigger occupancy sensor to change Occupancy attribute value to 1 => TESTER ACTION on DUT
            self.wait_for_user_input(prompt_msg="Type any letter and press ENTER after a sensor occupancy is triggered.")

        # And then check if Occupancy attribute has changed.
        occupancy_dut = await self.read_occ_attribute_expect_success(attribute=attributes.Occupancy)
        asserts.assert_equal(occupancy_dut, 1, "Occupancy state is not changed to 1")

        # subscription verification
        await_sequence_of_reports(report_queue=attrib_listener.attribute_queue, endpoint_id=endpoint_id, attribute=cluster.Attributes.Occupancy, sequence=[
                          1], timeout_sec=post_prompt_settle_delay_seconds)

        self.step(5)
        # check if Occupancy attribute is back to 0 after HoldTime attribute period
        # Tester should not be triggering the sensor for this test step.
        if has_hold_time:

            # Start a timer based on HoldTime
            time.sleep(hold_time + 2.0)  # add some extra 2 seconds to ensure hold time has passed.

            occupancy_dut = await self.read_occ_attribute_expect_success(attribute=attributes.Occupancy)
            asserts.assert_equal(occupancy_dut, 0, "Occupancy state is not 0 after HoldTime period")

        else:
            logging.info("HoldTime attribute not supported. Skip this return to 0 timing test procedure.")
            self.skip_step(5)


if __name__ == "__main__":
    default_matter_test_main()
