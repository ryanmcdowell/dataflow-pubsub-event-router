/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.pso.pipeline;

import com.google.cloud.pso.config.Configuration;
import com.google.cloud.pso.config.EventRouterConfig;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * The {@link PubsubEventRouter} is a pipeline which re-routes Pub/Sub messages to different topics
 * based on a message attribute. The router republishes the messages with their original event
 * timestamp in a new attribute, "ts", so the next Pub/Sub consumer can move the element into the
 * proper window.
 */
public class PubsubEventRouter {

  /*
   * The attribute which will hold the event time of the message.
   */
  private static final String EVENT_TIME_ATTR = "ts";

  /*
   * The attribute which holds the event type for the message.
   */
  private static final String EVENT_TYPE_ATTR = "type";

  /*
   * The location of the router config within the resources.
   */
  private static final String ROUTER_CONFIG_FILE = "router.yaml";

  /**
   * Options supported by this pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {

    @Description("The Pub/Sub subscription to read events from.")
    @Required
    String getInputSubscription();
    void setInputSubscription(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Load the configuration from resources
    Configuration config = loadConfig();

    // Create a tuple tag for each event type
    TupleTag<Void> mainOutTag = new TupleTag<Void>() {
    };
    Map<EventRouterConfig, TupleTag<PubsubMessage>> eventTags = createTags(config);

    /*
     * Steps:
     *  1) Read the events from the source topic
     *  2) Route each event type in the configuration to a different output
     *  3) Write each output to the associated topic for that event type
     */
    PCollectionTuple routerOut = pipeline
        .apply(
            "ReadEvents",
            PubsubIO
                .readMessagesWithAttributes()
                .fromSubscription(options.getInputSubscription()))
        .apply(
            "RouteEvent",
            ParDo
                .of(new Router(eventTags))
                .withOutputTags(
                    mainOutTag,
                    TupleTagList.of(new ArrayList<>(eventTags.values()))));

    // Loop through the eventTags and add a Pub/Sub output for each one.
    eventTags.forEach((eventRouterConfig, tag) ->
        routerOut
            .get(tag)
            .apply(
                "Write " + eventRouterConfig.getEventName(),
                PubsubIO
                    .writeMessages()
                    .to(eventRouterConfig.getTopic())
                    .withTimestampAttribute(EVENT_TIME_ATTR)));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Loads the configuration of event types mapped to their associated topics from
   * the resources directory.
   * @return The loaded configuration.
   */
  private static Configuration loadConfig() {
    Configuration.loadConfig(ROUTER_CONFIG_FILE);
    return Configuration.getInstance();
  }

  /**
   * Maps each event configuration to a {@link TupleTag} to allow the outputs to be properly
   * configured within the pipeline.
   * @param config The configuration loaded from the resources directory.
   */
  private static Map<EventRouterConfig, TupleTag<PubsubMessage>> createTags(Configuration config) {
    Map<EventRouterConfig, TupleTag<PubsubMessage>> tags = Maps.newHashMap();
    config.getEventRouters()
        .forEach(eventRouterConfig ->
            tags.put(eventRouterConfig, new TupleTag<PubsubMessage>() {
            }));

    return tags;
  }

  /**
   * The {@link Router} class re-routes messages to different outputs based on the event type read
   * from the message attributes.
   */
  static class Router extends DoFn<PubsubMessage, Void> {

    private final Map<EventRouterConfig, TupleTag<PubsubMessage>> tags;

    public Router(Map<EventRouterConfig, TupleTag<PubsubMessage>> tags) {
      this.tags = tags;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();

      // Add the event time to the attribute map so the event can be placed into the proper
      // window after routing.
      Map<String, String> attrs = new HashMap<>(message.getAttributeMap());
      attrs.put(EVENT_TIME_ATTR, Long.toString(context.timestamp().getMillis()));

      // Route the event type by finding the match event configuration
      // TODO: Add dead-letter for unmatched events
      String eventType = message.getAttribute(EVENT_TYPE_ATTR);
      if (eventType != null) {
        tags.forEach(((eventRouterConfig, tag) -> {
          if (eventType.equals(eventRouterConfig.getEventName())) {
            context.output(tag, new PubsubMessage(message.getPayload(), attrs));
          }
        }));
      }
    }
  }
}
