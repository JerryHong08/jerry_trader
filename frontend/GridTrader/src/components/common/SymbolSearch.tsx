import React, { useState, useRef, useEffect } from 'react';
import { Search, X, Check } from 'lucide-react';

interface SymbolSearchProps {
  value: string;
  onChange: (symbol: string) => void;
  availableSymbols?: string[];
  placeholder?: string;
  showCurrentSymbol?: boolean;
  syncGroup?: string | null;
  useConfirmButton?: boolean;
}

export function SymbolSearch({
  value,
  onChange,
  availableSymbols = [],
  placeholder = 'Search symbol...',
  showCurrentSymbol = false,
  syncGroup,
  useConfirmButton = false,
}: SymbolSearchProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [pendingSymbol, setPendingSymbol] = useState('');
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
        setSearchTerm('');
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const filteredSymbols = availableSymbols.filter(symbol =>
    symbol.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleSelect = (symbol: string) => {
    if (useConfirmButton) {
      setPendingSymbol(symbol);
      setSearchTerm(symbol);
      setIsOpen(false);
    } else {
      onChange(symbol);
      setIsOpen(false);
      setSearchTerm('');
    }
  };

  const handleConfirm = () => {
    if (pendingSymbol) {
      onChange(pendingSymbol);
      setPendingSymbol('');
      setSearchTerm('');
    } else if (searchTerm) {
      onChange(searchTerm.toUpperCase());
      setSearchTerm('');
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      if (useConfirmButton) {
        handleConfirm();
        setIsOpen(false); // Close dropdown after Enter
      } else if (filteredSymbols.length > 0) {
        handleSelect(filteredSymbols[0]);
        setIsOpen(false); // Close dropdown after Enter
      }
    }
  };

  return (
    <div className="relative" ref={dropdownRef}>
      <div className="flex items-center gap-2">
        {/* Search Input */}
        <div className="relative flex-1">
          <input
            type="text"
            value={searchTerm}
            onChange={(e) => {
              setSearchTerm(e.target.value);
              setPendingSymbol('');
              setIsOpen(true);
            }}
            onFocus={() => setIsOpen(true)}
            onKeyDown={handleKeyDown}
            placeholder={placeholder}
            className="w-full px-3 py-1.5 pl-9 bg-zinc-800 border border-zinc-700 focus:border-zinc-600 focus:outline-none text-sm"
          />
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
        </div>
      </div>

      {/* Dropdown */}
      {isOpen && filteredSymbols.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-zinc-800 border border-zinc-700 max-h-60 overflow-y-auto z-50 shadow-lg">
          {filteredSymbols.map((symbol) => (
            <button
              key={symbol}
              onClick={() => handleSelect(symbol)}
              className="w-full text-left px-3 py-2 hover:bg-zinc-700 transition-colors text-sm"
            >
              {symbol}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
