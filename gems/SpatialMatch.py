from dataclasses import dataclass
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class SpatialMatch(MacroSpec):
    name: str = "SpatialMatch"
    projectName: str = "DatabricksSqlSpatial"
    category: str = "Spatial"
    minNumOfInputPorts: int = 2

    @dataclass(frozen=True)
    class SpatialMatchProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schemas: List[List[str]] = field(default_factory=list)   
        source_column:str = ""
        target_column:str = ""
        match_type: str = ""


    def get_relation_names(self, component: Component, context: SqlContext):
        all_upstream_nodes = []
        for inputPort in component.ports.inputs:
            upstreamNode = None
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
            all_upstream_nodes.append(upstreamNode)

        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.label is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.label)

        return relation_name
        
    def extract_schemas(self, component: Component):
        """Return list[str] one compact JSON blob per input port."""
        schemas = []
        for inputPort in component.ports.inputs:
            raw_schema = json.loads(str(inputPort.schema).replace("'", '"'))
            fields_arr = [str(f["name"]) for f in raw_schema["fields"]]
            schemas.append(fields_arr)
        return schemas


    def dialog(self) -> Dialog:
        dialog = Dialog("SpatialMatch") \
            .addElement(
                ColumnsLayout(gap="1rem", height="auto")
                    .addColumn(Ports(), "content")
                    .addColumn(
                    StackLayout(height="100%")
                        .addElement(
                            AlertBox(
                                variant="warning",
                                _children=[
                                    Markdown(
                                        "**This Gem uses Databricks Spatial SQL features currently in Private Preview.**\n\n"
                                        "To enable these capabilities, please contact your Databricks representative. For more information, see the [Databricks Preview Feature Documentation](https://docs.databricks.com/en/admin/workspace-settings/manage-previews.html)."
                                    )
                                ]
                            )
                        )
                        .addElement(
                        StepContainer()
                            .addElement(
                            Step()
                                .addElement(
                                StackLayout(height="100%")
                                .addElement(
                                    TitleElement("Spatial Object Fields")
                                )
                                .addElement(
                                    AlertBox(
                                        variant="info",
                                        _children=[
                                            Markdown(
                                                "**Geometry Convention**\n\n"
                                                "Use **Source** for smaller geometries (e.g., points or lines) and **Target** for larger shapes (e.g., polygons) ."
                                                "This ensures spatial matches like **within** and **intersects** work as expected."
                                            )
                                        ]
                                    )
                                )
                                    .addElement(
                                    ColumnsLayout(gap="1rem", height="100%")
                                        .addColumn(
                                            SchemaColumnsDropdown("Source Column")
                                        .bindSchema("component.ports.inputs[0].schema")
                                        .bindProperty("source_column")
                                    )
                                        .addColumn(
                                            SchemaColumnsDropdown("Target Column")
                                        .bindSchema("component.ports.inputs[1].schema")
                                        .bindProperty("target_column")
                                    )
                                )
                            )
                        )
                    )
                    .addElement(
                        StackLayout(height="100%")
                            .addElement(
                                StepContainer()
                                .addElement(
                                    Step()
                                    .addElement(
                                        StackLayout(height="auto")
                                        .addElement(
                                            SelectBox("Select Match Type")
                                            .addOption("Source Intersects Target", "intersects")
                                            .addOption("Source Contains Target", "contains")
                                            .addOption("Source Within Target", "within")
                                            .addOption("Source Touches Target", "touches")
                                            .addOption("Source Touches or Intersects Target", "touches_or_intersects")
                                            .addOption("Source Envelope Overlaps Target Envelope", "envelope")
                                            .bindProperty("match_type")
                                        )
                                    )
                                )
                            )
                        )
                        .addElement(
                            TitleElement("Match Types")
                        )
                        .addElement(
                            AlertBox(
                                variant="info",
                                _children=[
                                    Markdown(
                                        "![alt text](docs/images/spatial_match_examples.svg)"
                                    )
                                ]
                            )
                        )
                        .addElement(
                            AlertBox(
                                variant="success",
                                _children=[
                                    Markdown(
                                        "**Things to consider** \n\n"
                                        "* **Source Column & Target Column** requires fields to be present in **WKT(well-known-text)** format \n\n"
                                        "* Use the **CreatePoint** Gem to convert **longitude** and **latitude** column pairs into **WKT (Well-Known Text) format** for easy geospatial processing \n\n"
                                        "* **Example**: If your table has columns like **from_longitude**, **from_latitude**, **to_longitude**, and **to_latitude**, first use the **CreatePoint Gem** to generate **from_geo_point** and **to_geo_point** in WKT format.\n\n"
                                    )
                                ]
                            )
                        )

                    )
            )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(SpatialMatch, self).validate(context, component)
        if len(component.properties.source_column) == 0:
            diagnostics.append(
                Diagnostic("component.properties.source_column", f"Please select a source column",
                           SeverityLevelEnum.Error)
            )

        if len(component.properties.target_column) == 0:
            diagnostics.append(
                Diagnostic("component.properties.target_column", f"Please select a target column",
                           SeverityLevelEnum.Error)
            )

        source_field_names = [field["name"] for field in component.ports.inputs[0].schema["fields"]]
        target_field_names = [field["name"] for field in component.ports.inputs[1].schema["fields"]]

        if len(component.properties.source_column) > 0:
            if component.properties.source_column not in source_field_names:
                diagnostics.append(
                    Diagnostic("component.properties.source_column",
                               f"Selected column {component.properties.source_column} is not present in input schema.",
                               SeverityLevelEnum.Error))

        if len(component.properties.target_column) > 0:
            if component.properties.target_column not in target_field_names:
                diagnostics.append(
                    Diagnostic("component.properties.target_column",
                               f"Selected column {component.properties.target_column} is not present in input schema.",
                               SeverityLevelEnum.Error))


        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=relation_name,
            schemas=self.extract_schemas(newState)
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: SpatialMatchProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            str(props.relation_name),
            str(props.schemas),
            "'" + props.source_column + "'",
            "'" + props.target_column + "'",
            "'" + props.match_type + "'"
        ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:

        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return SpatialMatch.SpatialMatchProperties(
            relation_name=parametersMap.get('relation_name'),
            schemas=parametersMap.get("schemas"),
            match_type=parametersMap.get('match_type'),
            source_column=parametersMap.get('source_column'),
            target_column=parametersMap.get('target_column')                          
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schemas",       str(properties.schemas)),
                MacroParameter("match_type", str(properties.match_type)),
                MacroParameter("source_column", str(properties.source_column)),
                MacroParameter("target_column", str(properties.target_column))                                
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Handle changes in the component's state and return the new state
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name,
            schemas=self._extract_schemas(newState)
        )
        return component.bindProperties(newProperties)
