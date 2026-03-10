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
      "width": 1010,
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
          "vwap",
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
          "vwap": 50,
          "marketCap": 110
        },
        "columnOrder": [
          "symbol",
          "state",
          "news",
          "price",
          "vwap",
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
      "width": 680,
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
        "view": "news"
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
      "width": 1010,
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
        "timeRange": "1h"
      }
    }
  },
  {
    "id": "chart-2",
    "moduleType": "chart",
    "position": {
      "x": 700,
      "y": 1890
    },
    "size": {
      "width": 690,
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
      "width": 760,
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
    "id": "rank-1",
    "moduleType": "rank-list",
    "position": {
      "x": 0,
      "y": 0
    },
    "size": {
      "width": 890,
      "height": 930
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
          "vwap",
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
          "vwap": 50,
          "marketCap": 110
        },
        "columnOrder": [
          "symbol",
          "state",
          "news",
          "price",
          "vwap",
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
    "id": "overview-chart-1",
    "moduleType": "overview-chart",
    "position": {
      "x": 900,
      "y": 0
    },
    "size": {
      "width": 760,
      "height": 530
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
      "x": 0,
      "y": 940
    },
    "size": {
      "width": 680,
      "height": 670
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
      "x": 1300,
      "y": 1620
    },
    "size": {
      "width": 360,
      "height": 260
    },
    "syncGroup": "group-5"
  },
  {
    "id": "news-room-1773115113599",
    "moduleType": "news-room",
    "position": {
      "x": 900,
      "y": 540
    },
    "size": {
      "width": 440,
      "height": 390
    },
    "syncGroup": null
  },
  {
    "id": "stock-detail-1773117146516",
    "moduleType": "stock-detail",
    "position": {
      "x": 1350,
      "y": 540
    },
    "size": {
      "width": 310,
      "height": 390
    },
    "syncGroup": "group-1"
  },
  {
    "id": "order-management-1773117184168",
    "moduleType": "order-management",
    "position": {
      "x": 1300,
      "y": 940
    },
    "size": {
      "width": 360,
      "height": 670
    },
    "syncGroup": "group-1"
  },
  {
    "id": "chart-1773117208665",
    "moduleType": "chart",
    "position": {
      "x": 690,
      "y": 940
    },
    "size": {
      "width": 600,
      "height": 670
    },
    "syncGroup": "group-1",
    "settings": {
      "chart": {
        "timeframe": "1h"
      }
    }
  },
  {
    "id": "overview-chart-1773117277779",
    "moduleType": "overview-chart",
    "position": {
      "x": 0,
      "y": 1620
    },
    "size": {
      "width": 680,
      "height": 260
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
        "focusMode": true
      }
    }
  },
  {
    "id": "order-management-1773117566809",
    "moduleType": "order-management",
    "position": {
      "x": 690,
      "y": 1620
    },
    "size": {
      "width": 600,
      "height": 260
    },
    "syncGroup": "group-1",
    "settings": {
      "orderManagement": {
        "view": "orders"
      }
    }
    }
  ],
};

// Template 3: Trade-Layout
export const Trade_TEMPLATE: LayoutTemplate = {
  id: "trade layout",
  name: "Trade-Layout",
  description: "trade layout for my laptop.",
  layout: [
  {
    "id": "order-1",
    "moduleType": "order-management",
    "position": {
      "x": 760,
      "y": 0
    },
    "size": {
      "width": 350,
      "height": 620
    },
    "syncGroup": "group-2",
    "settings": {
      "orderManagement": {
        "view": "placement"
      }
    }
  },
  {
    "id": "stock-detail-1",
    "moduleType": "stock-detail",
    "position": {
      "x": 1120,
      "y": 0
    },
    "size": {
      "width": 550,
      "height": 620
    },
    "syncGroup": "group-2",
    "settings": {
      "stockDetail": {
        "view": "fundamentals"
      }
    }
  },
  {
    "id": "chart-2",
    "moduleType": "chart",
    "position": {
      "x": 0,
      "y": 400
    },
    "size": {
      "width": 750,
      "height": 520
    },
    "syncGroup": "group-2",
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
      "x": 0,
      "y": 0
    },
    "size": {
      "width": 750,
      "height": 390
    },
    "syncGroup": "group-2"
  },
  {
    "id": "order-management-1772196836934",
    "moduleType": "order-management",
    "position": {
      "x": 760,
      "y": 630
    },
    "size": {
      "width": 910,
      "height": 290
    },
    "syncGroup": "group-2",
    "settings": {
      "orderManagement": {
        "view": "orders"
      }
    }
    }
  ]
};

// Export all templates as a Record (object) instead of array
export const LAYOUT_TEMPLATES: Record<string, LayoutTemplate> =
  {
    "default-trading": DEFAULT_TRADING_TEMPLATE,
    "minimal layout": MINIMAL_TEMPLATE,
    "trade layout": Trade_TEMPLATE,
  };

// Get all templates as an array for UI rendering
export const getAllTemplates = (): LayoutTemplate[] => {
  return Object.values(LAYOUT_TEMPLATES);
};

// Default template ID
export const DEFAULT_TEMPLATE_ID = "default-trading";
