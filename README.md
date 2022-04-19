# Generating Cesium assets from iSamples record

This repo contains tools that can be used to generate a PointCloud from iSamples records. 

The basic workflow is:

1. Get all points along with source, material type, specimen type, sampled feature
2. Compute elevations for distinct locations
3. Assign Z values to each point
4. Generate `.las` file from points using `pdal`
5. Upload `.las` to Cesium ion to create point tiles

## Environment Setup
Using homebrew, install:

```
brew install pdal
```

Note: for my installation it was necessary to create a symlink `libgdal.29.dylib`:

```
cd /usr/local/opt/gdal/lib/
ln -s libgdal.30.dylib libgdal.29.dylib
```

This is not recommended because of the version difference. It should be reconciled when the maintainers of `pdal` update dependencies.

## Get points from iSamples

Use the streaming API. Requests resulting in more than 500k points requires direct connection to solr, which can be done via ssh port forward.

```
https://hyde.cyverse.org/isamples_central/thing/stream?fl=producedBy_resultTimeRange%20hasContextCategory%20id%20keywords%20hasMaterialCategory%20registrant%20source%20hasSpecimenCategory&q=searchText%3Amoorea&fq=source%3A(%22SESAR%22)&rows=1000&wt=jsonl
```

To generate the sqlite database `records.sqlite`:

```
python loadpoints.py -v INFO get
```

This process takes a long time on initial run. Subsequent runs can use a query on index time to limit the records retrieved to just those added to the index since the last download.

## Compute elevations for distinct locations

Use the `elevate` node project. e.g.:

```
$ export TOKEN="eyJhbG..."
$ node elevate.js -k $TOKEN -f ../isamples/source/isamples_cesium/records.sqlite
Total rows to calculate = 2302
Total pages to process = 3
Points remaining: 2302
Page 1 loading...
Page 1 computing...
Page 1 saving...
Updated 1000 points
...
Points remaining: 0
Done.
```

## Create a `.las` file from points and elevation data

```
python loadpoints.py csv > records.csv
pdal pipeline pipeline.json
```

### `las` property mapping

These are the only per-point properties available in Cesium `las` to point cloud conversion:

| las | bytes | Source |
| --- | --- | ---- |
| X | long | longitude |
| Y | long | latitude |
| Z | long | surface elevation + n*COUNT_SCALE |
| classification | unsigned char  | data source |
| intensity | unsigned short |  |
| red | unsigned short |  |
| green | unsigned short |  |
| blue | unsigned short |  |

```
long: −2,147,483,647, +2,147,483,647
unsigned char: 0 - 255
unsigned short: 0 - 65,535
```

$intensity = scale(sample_date - epoch)$
```python
>>> epoch = datetime.datetime(2029,12,31).toordinal()-65535
>>> epoch
675542
>>> datetime.datetime.fromordinal(epoch)
datetime.datetime(1850, 7, 28, 0, 0)
```

Material type has 19 distinct values. Sampled Feature has 18. Specimen Type has 16. 19 needs 5 bits. Using a full 5 bits provides a maximum value of 31. So each of the vocabularies could be expressed in 5 bits, so all three could be provided by a single unsigned short value.

```
encoded = v0 + (v1 << 5) + (v2 << 5)

v0 = encoded & 31
v1 = encoded & (31 << 5)
v2 = encoded & (31 << 10)
```

## Upload to Cesium and view result


Minimal cesium viewer for records, adjust the asset id:
```javascript
// Grant CesiumJS access to your ion assets
Cesium.Ion.defaultAccessToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIwNzk3NjkyMy1iNGI1LTRkN2UtODRiMy04OTYwYWE0N2M3ZTkiLCJpZCI6Njk1MTcsImlhdCI6MTYzMzU0MTQ3N30.e70dpNzOCDRLDGxRguQCC-tRzGzA-23Xgno5lNgCeB4";

const worldTerrain = Cesium.createWorldTerrain({
    //requestWaterMask: true,
    //requestVertexNormals: true,
});

const viewer = new Cesium.Viewer("cesiumContainer",
  {
    terrainProvider: worldTerrain,
  }
);


function addPointsBySource(assetId) {
  const tileset = viewer.scene.primitives.add(new Cesium.Cesium3DTileset({
    url: Cesium.IonResource.fromAssetId(assetId),
    depthFailMaterial: new Cesium.PolylineOutlineMaterialProperty(
        {
          color: Cesium.Color.RED,
        }
      ),            
  }));
  tileset.style = new Cesium.Cesium3DTileStyle({
    color: {
      conditions: [
          ["${Classification} === 0", "color('purple')"],
          ["${Classification} === 1", "color('brown')"],
          ["${Classification} === 2", "color('cyan')"],
          ["${Classification} === 3", "color('orange')"],
          ["${Classification} === 4", "color('green')"],              
          ["true", "color('white')"]
      ]
    },
    pointSize: 5,
    zIndex: 100,
  });
  return tileset;
}


const tileset = addPointsBySource(897325);

(async () => {
  try {
    await tileset.readyPromise;
    await viewer.zoomTo(tileset);

    // Apply the default style if it exists
    var extras = tileset.asset.extras;
    if (
      Cesium.defined(extras) &&
      Cesium.defined(extras.ion) &&
      Cesium.defined(extras.ion.defaultStyle)
    ) {
      tileset.style = new Cesium.Cesium3DTileStyle(extras.ion.defaultStyle);
    }
  } catch (error) {
    console.log(error);
  }
})();

```

### Selecting points

You can't select individual points in a point cloud. However, we can fake it by styling to show points in the vicinity of the cursor:

```
1. get the point of interest (cursor position in space)
2. set color / size using distance to POI
3. Use the coordinates of the cursor in earth coordinates to query the index
```

## References

* [Discussion on las properties available](https://community.cesium.com/t/available-variables-from-las-file/15884/3)
* [Example for point cloud styling](https://sandcastle.cesium.com/?src=3D%20Tiles%20Point%20Cloud%20Styling.html&label=3D%20Tiles)
* [LAS file format](http://www.asprs.org/wp-content/uploads/2019/03/LAS_1_4_r14.pdf)
* [PDAL LAS writer](https://pdal.io/stages/writers.las.html#writers-las)
