<#--

     SPDX-License-Identifier: Apache-2.0

     Copyright The original authors

     Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->

Third-party dependencies bundled with this distribution and their licenses.

This binary includes components from GraalVM Community Edition and OpenJDK,
licensed under the GNU General Public License, version 2 with the Classpath
Exception, which permits distribution of linked executables without requiring
the application code to be licensed under the GPL.

Libraries originally dual-licensed under the Eclipse Public License 2.0 and
the GNU General Public License, version 2 with the GNU Classpath Exception
are redistributed here under the Eclipse Public License 2.0.
<#list licenseMap as entry>

${entry.getKey()}
<#list entry.getValue() as project>
    ${project.name} (${project.groupId}:${project.artifactId}:${project.version} - ${(project.url)!"no url defined"})
</#list>
</#list>
