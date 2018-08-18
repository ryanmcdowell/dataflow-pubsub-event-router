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

package com.google.cloud.pso.config;

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import org.yaml.snakeyaml.Yaml;

/**
 * A singleton config which holds the router settings.
 */
public final class Configuration {

  /**
   * The instance of the Configuration that this class is storing.
   */
  private static Configuration instance = null;

  /**
   * Get the Instance of this class. There should only ever be one instance of this class and other
   * classes can use this static method to retrieve the instance.
   *
   * @return Configuration the stored Instance of this class
   */
  public static Configuration getInstance() {
    if (Configuration.instance == null) {
      throw new IllegalStateException(
          "The configuration must be loaded with .load() prior to access!");
    }

    return Configuration.instance;
  }

  private List<EventRouterConfig> eventRouters;

  public List<EventRouterConfig> getEventRouters() {
    return eventRouters;
  }

  public void setEventRouters(List<EventRouterConfig> eventRouters) {
    this.eventRouters = eventRouters;
  }

  public static void setInstance(Configuration instance) {
    Configuration.instance = instance;
  }

  /**
   * Load the Configuration specified at fileName.
   *
   * @return boolean did this load succeed?
   */
  public static boolean loadConfig(String filename) {
    try {
      String configYaml = Resources.toString(
          Resources.getResource(filename), Charset.defaultCharset());

      Yaml yaml = new Yaml();
      instance = yaml.loadAs(configYaml, Configuration.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return true;
  }
}
