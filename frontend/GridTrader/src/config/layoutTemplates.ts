import type { GridItemConfig } from "../types";

export interface LayoutTemplate {
  id: string;
  name: string;
  description: string;
  layout: GridItemConfig[];
}

// Template 1: Default Trading Layout
export const DEFAULT_TRADING_TEMPLATE: LayoutTemplate = {
  id: "default-trading",
  name: "Default Trading",
  description:
    "Standard layout with order management, rank list, charts, and stock details",
  layout: [
    {
      "id": "order-1",
      "moduleType": "order-management",
      "position": {
        "x": 10,
        "y": 350
      },
      "size": {
        "width": 360,
        "height": 630
      },
      "syncGroup": "group-1",
      "settings": {
        "orderManagement": {
          "view": "placement"
        }
      }
    },
    {
      "id": "rank-1",
      "moduleType": "rank-list",
      "position": {
        "x": 380,
        "y": 350
      },
      "size": {
        "width": 1000,
        "height": 840
      },
      "syncGroup": "group-1",
      "settings": {
        "rankList": {
          "sortColumn": "state",
          "sortDirection": "desc",
          "visibleColumns": [
            "news",
            "price",
            "changePercent",
            "volume",
            "float",
            "relativeVolumeDaily",
            "relativeVolume5min",
            "marketCap",
            "state",
            "symbol"
          ],
          "columnWidths": {
            "#": 50,
            "symbol": 50,
            "state": 99,
            "news": 50,
            "price": 50,
            "change": 50,
            "changePercent": 110,
            "volume": 50,
            "float": 50,
            "relativeVolumeDaily": 108,
            "relativeVolume5min": 110,
            "marketCap": 110
          },
          "columnOrder": [
            "symbol",
            "state",
            "news",
            "price",
            "changePercent",
            "float",
            "change",
            "volume",
            "relativeVolumeDaily",
            "relativeVolume5min",
            "marketCap"
          ]
        }
      }
    },
    {
      "id": "chart-1",
      "moduleType": "chart",
      "position": {
        "x": 10,
        "y": 1890
      },
      "size": {
        "width": 660,
        "height": 540
      },
      "syncGroup": "group-1",
      "settings": {
        "chart": {
          "timeframe": "1D"
        }
      }
    },
    {
      "id": "stock-detail-1",
      "moduleType": "stock-detail",
      "position": {
        "x": 10,
        "y": 990
      },
      "size": {
        "width": 360,
        "height": 890
      },
      "syncGroup": "group-1",
      "settings": {
        "stockDetail": {
          "view": "fundamentals"
        }
      }
    },
    {
      "id": "overview-chart-1768826532211",
      "moduleType": "overview-chart",
      "position": {
        "x": 380,
        "y": 1200
      },
      "size": {
        "width": 1000,
        "height": 680
      },
      "syncGroup": "group-1",
      "settings": {
        "overviewChart": {
          "selectedStates": [
            "Best",
            "Good",
            "OnWatch",
            "NotGood",
            "Bad"
          ],
          "focusMode": false,
          "timeRange": "30m"
        }
      }
    },
    {
      "id": "chart-2",
      "moduleType": "chart",
      "position": {
        "x": 680,
        "y": 1890
      },
      "size": {
        "width": 700,
        "height": 540
      },
      "syncGroup": "group-1",
      "settings": {
        "chart": {
          "timeframe": "1h"
        }
      }
    },
    {
      "id": "order-management-2",
      "moduleType": "order-management",
      "position": {
        "x": 630,
        "y": 0
      },
      "size": {
        "width": 750,
        "height": 340
      },
      "syncGroup": "group-5",
      "settings": {
        "orderManagement": {
          "view": "orders"
        }
      }
    },
    {
      "id": "portfolio-1767513971231",
      "moduleType": "portfolio",
      "position": {
        "x": 10,
        "y": 0
      },
      "size": {
        "width": 610,
        "height": 340
      },
      "syncGroup": "group-5"
    }
  ]
};

// Template 2: Minimal-Layout
export const MINIMAL_TEMPLATE: LayoutTemplate = {
  id: "minimal layout",
  name: "Minimal-Layout",
  description: "minimal layout for my laptop.",
  layout: [
    {
      "id": "order-1",
      "moduleType": "order-management",
      "position": {
        "x": 900,
        "y": 650
      },
      "size": {
        "width": 350,
        "height": 290
      },
      "syncGroup": "group-5",
      "settings": {
        "orderManagement": {
          "view": "placement"
        }
      }
    },
    {
      "id": "rank-1",
      "moduleType": "rank-list",
      "position": {
        "x": 0,
        "y": 0
      },
      "size": {
        "width": 890,
        "height": 410
      },
      "syncGroup": "group-1",
      "settings": {
        "rankList": {
          "sortColumn": "state",
          "sortDirection": "desc",
          "visibleColumns": [
            "news",
            "price",
            "changePercent",
            "volume",
            "float",
            "relativeVolumeDaily",
            "relativeVolume5min",
            "marketCap",
            "state",
            "symbol"
          ],
          "columnWidths": {
            "#": 50,
            "symbol": 50,
            "state": 94,
            "news": 50,
            "price": 50,
            "change": 50,
            "changePercent": 110,
            "volume": 50,
            "float": 50,
            "relativeVolumeDaily": 108,
            "relativeVolume5min": 110,
            "marketCap": 110
          },
          "columnOrder": [
            "symbol",
            "state",
            "news",
            "price",
            "changePercent",
            "float",
            "change",
            "volume",
            "relativeVolumeDaily",
            "relativeVolume5min",
            "marketCap"
          ]
        }
      }
    },
    {
      "id": "stock-detail-1",
      "moduleType": "stock-detail",
      "position": {
        "x": 0,
        "y": 420
      },
      "size": {
        "width": 350,
        "height": 520
      },
      "syncGroup": "group-1",
      "settings": {
        "stockDetail": {
          "view": "news"
        }
      }
    },
    {
      "id": "overview-chart-1",
      "moduleType": "overview-chart",
      "position": {
        "x": 900,
        "y": 0
      },
      "size": {
        "width": 760,
        "height": 640
      },
      "syncGroup": "group-1",
      "settings": {
        "overviewChart": {
          "selectedStates": [
            "Best",
            "Good",
            "OnWatch",
            "NotGood",
            "Bad"
          ],
          "focusMode": false
        }
      }
    },
    {
      "id": "chart-2",
      "moduleType": "chart",
      "position": {
        "x": 360,
        "y": 420
      },
      "size": {
        "width": 530,
        "height": 520
      },
      "syncGroup": "group-1",
      "settings": {
        "chart": {
          "timeframe": "1m"
        }
      }
    },
    {
      "id": "portfolio-1767537783678",
      "moduleType": "portfolio",
      "position": {
        "x": 1260,
        "y": 650
      },
      "size": {
        "width": 400,
        "height": 290
      },
      "syncGroup": "group-5"
    }
  ],
};

// Template 3: Test-Layout
export const Test_TEMPLATE: LayoutTemplate = {
  id: "test layout",
  name: "Test-Layout",
  description: "test layout for my laptop.",
  layout: [
    {
      "id": "order-1",
      "moduleType": "order-management",
      "position": {
        "x": 10,
        "y": 350
      },
      "size": {
        "width": 360,
        "height": 630
      },
      "syncGroup": "group-1",
      "settings": {
        "orderManagement": {
          "view": "placement"
        }
      }
    },
    {
      "id": "rank-1",
      "moduleType": "rank-list",
      "position": {
        "x": 380,
        "y": 350
      },
      "size": {
        "width": 1000,
        "height": 840
      },
      "syncGroup": "group-1",
      "settings": {
        "rankList": {
          "sortColumn": "changePercent",
          "sortDirection": "desc",
          "visibleColumns": [
            "news",
            "price",
            "changePercent",
            "volume",
            "float",
            "relativeVolumeDaily",
            "relativeVolume5min",
            "marketCap",
            "state",
            "symbol"
          ],
          "columnWidths": {
            "#": 50,
            "symbol": 50,
            "state": 99,
            "news": 50,
            "price": 50,
            "change": 50,
            "changePercent": 110,
            "volume": 50,
            "float": 50,
            "relativeVolumeDaily": 108,
            "relativeVolume5min": 110,
            "marketCap": 110
          },
          "columnOrder": [
            "symbol",
            "state",
            "news",
            "price",
            "changePercent",
            "float",
            "change",
            "volume",
            "relativeVolumeDaily",
            "relativeVolume5min",
            "marketCap"
          ]
        }
      }
    },
    {
      "id": "stock-detail-1",
      "moduleType": "stock-detail",
      "position": {
        "x": 10,
        "y": 990
      },
      "size": {
        "width": 360,
        "height": 1450
      },
      "syncGroup": "group-1",
      "settings": {
        "stockDetail": {
          "view": "fundamentals"
        }
      }
    },
    {
      "id": "order-management-2",
      "moduleType": "order-management",
      "position": {
        "x": 630,
        "y": 0
      },
      "size": {
        "width": 750,
        "height": 340
      },
      "syncGroup": "group-5",
      "settings": {
        "orderManagement": {
          "view": "orders"
        }
      }
    },
    {
      "id": "portfolio-1767513971231",
      "moduleType": "portfolio",
      "position": {
        "x": 10,
        "y": 0
      },
      "size": {
        "width": 610,
        "height": 340
      },
      "syncGroup": "group-5"
    },
    {
      "id": "overview-chart-1768823876522",
      "moduleType": "overview-chart",
      "position": {
        "x": 380,
        "y": 1200
      },
      "size": {
        "width": 1000,
        "height": 680
      },
      "syncGroup": "group-1",
      "settings": {
        "overviewChart": {
          "selectedStates": [
            "Best",
            "Good",
            "OnWatch",
            "NotGood",
            "Bad"
          ],
          "focusMode": true,
          "timeRange": "30m"
        }
      }
    },
    {
      "id": "overview-chart-1768826532211",
      "moduleType": "overview-chart",
      "position": {
        "x": 380,
        "y": 1890
      },
      "size": {
        "width": 1000,
        "height": 550
      },
      "syncGroup": "group-1",
      "settings": {
        "overviewChart": {
          "selectedStates": [
            "Best",
            "Good",
            "OnWatch",
            "NotGood",
            "Bad"
          ],
          "focusMode": false,
          "timeRange": "30m"
        }
      }
    }
  ],
};

// Export all templates as a Record (object) instead of array
export const LAYOUT_TEMPLATES: Record<string, LayoutTemplate> =
  {
    "default-trading": DEFAULT_TRADING_TEMPLATE,
    "minimal layout": MINIMAL_TEMPLATE,
    "test layout": Test_TEMPLATE,
  };

// Get all templates as an array for UI rendering
export const getAllTemplates = (): LayoutTemplate[] => {
  return Object.values(LAYOUT_TEMPLATES);
};

// Default template ID
export const DEFAULT_TEMPLATE_ID = "default-trading";
