import React, { useState, useMemo, useRef, useEffect, useCallback } from "react";
import {
  Wind, Sun, Wine, Droplets, ShieldCheck, Star,
  MapPin, Phone, Clock, Navigation, Search,
  Calendar, MessageCircle, ChevronRight, CheckCircle2,
  X, SlidersHorizontal, Tag, Zap, MapPinned, DollarSign,
  Ticket, AlertCircle, Filter, ChevronDown, ChevronUp
} from "lucide-react";

var C = { navy:"#1B2A4A", red:"#E63946", bg:"#F4F5F7", card:"#FFFFFF", border:"#E8EAF0", muted:"#8B92A5", green:"#16a34a", shadow:"0 1px 4px rgba(27,42,74,0.06)", shadowHover:"0 8px 24px rgba(27,42,74,0.10)", radius:14 };
var font = '"Outfit", system-ui, sans-serif';

var FACILITIES = {
  angMoKio: { name:"Ang Mo Kio", address:"12 Ang Mo Kio Industrial Park 2, Singapore 569500", hours:"24/7 Access", office:"9AM–6PM", phone:"+65 6553 1333" },
  tai_seng: { name:"Tai Seng", address:"5 Tai Seng Drive, Singapore 535215", hours:"24/7 Access", office:"9AM–6PM", phone:"+65 6553 1334" },
  kallang: { name:"Kallang Way", address:"1 Kallang Way 2A, Singapore 347495", hours:"24/7 Access", office:"9AM–6PM", phone:"+65 6553 1335" },
};

var UNITS = [
  { id:"A1088", label:"S", dims:"5ft × 5ft", sqft:25, tags:["Near lift","Near MRT"], price:180, climate:"Air Con", facility:"angMoKio" },
  { id:"B2374", label:"S", dims:"5ft × 7ft", sqft:35, tags:["Near MRT"], price:210, climate:"Non Air Con", facility:"angMoKio" },
  { id:"C4921", label:"M", dims:"8ft × 10ft", sqft:80, tags:["Ground Floor"], price:450, climate:"Wine", facility:"angMoKio" },
  { id:"D8650", label:"XS", dims:"3ft × 3ft", sqft:9, tags:["Near lift"], price:95, climate:"Dehumidifier", facility:"angMoKio" },
  { id:"E1093", label:"L", dims:"10ft × 12ft", sqft:120, tags:["Wide Corridor"], price:620, climate:"Climate Controlled", facility:"angMoKio" },
  { id:"F7248", label:"S", dims:"5ft × 5ft", sqft:25, tags:["Near MRT","Near lift"], price:185, climate:"Air Con", facility:"angMoKio" },
  { id:"TS101", label:"S", dims:"5ft × 5ft", sqft:25, tags:["Near MRT"], price:155, climate:"Air Con", facility:"tai_seng" },
  { id:"TS202", label:"M", dims:"7ft × 8ft", sqft:56, tags:["Ground Floor"], price:340, climate:"Non Air Con", facility:"tai_seng" },
  { id:"KW301", label:"XS", dims:"3ft × 4ft", sqft:12, tags:["Near lift"], price:78, climate:"Air Con", facility:"kallang" },
  { id:"KW402", label:"L", dims:"10ft × 10ft", sqft:100, tags:["Drive-up"], price:520, climate:"Climate Controlled", facility:"kallang" },
];

var DISCOUNTS = [
  { id:1, name:"First Month Free", desc:"New customers, 3+ month contract.", pct:0, type:"first_free", badge:"POPULAR" },
  { id:2, name:"20% Off 6-Month Plan", desc:"Pre-pay 6 months to unlock.", pct:0.20, type:"percent" },
  { id:3, name:"10% Student Discount", desc:"Valid student ID at check-in.", pct:0.10, type:"percent" },
];

var VALID_COUPONS = { SAVE10:0.10, EXTRA15:0.15, WELCOME:0.05 };

var CLIMATES = [
  { id:"Air Con", icon:Wind, short:"AC" },
  { id:"Non Air Con", icon:Sun, short:"No AC" },
  { id:"Wine", icon:Wine, short:"Wine" },
  { id:"Dehumidifier", icon:Droplets, short:"Dehum" },
  { id:"Climate Controlled", icon:ShieldCheck, short:"Climate" },
  { id:"Executive", icon:Star, short:"Exec" },
  { id:"Premium AC", icon:Wind, short:"Prem" },
];

var SIZES = [
  { label:"All", range:[0,999] }, { label:"XS", sub:"<25", range:[0,24] },
  { label:"S", sub:"18-35", range:[18,35] }, { label:"M", sub:"36-80", range:[36,80] },
  { label:"L", sub:"81-145", range:[81,145] }, { label:"XL", sub:"146-300", range:[146,300] },
];

var MODES = [
  { id:"selection", label:"Your Selection", mLabel:"Yours", icon:SlidersHorizontal },
  { id:"best_price", label:"Best Price", mLabel:"Best $", icon:DollarSign },
  { id:"nearby", label:"Nearby", mLabel:"Nearby", icon:MapPinned },
  { id:"nearby_best", label:"Nearby Best $", mLabel:"Near $", icon:Zap },
];

function RangeSlider(props) {
  var min = props.min, max = props.max, value = props.value, onChange = props.onChange;
  var ref = useRef(null);
  var _d = useState(null); var drag = _d[0]; var setDrag = _d[1];
  var pct = function(v) { return ((v - min) / (max - min)) * 100; };
  useEffect(function() {
    if (!drag) return;
    function move(e) { var r = ref.current.getBoundingClientRect(); var x = e.touches ? e.touches[0].clientX : e.clientX; var ratio = Math.max(0, Math.min(1, (x - r.left) / r.width)); var val = Math.round(min + ratio * (max - min)); if (drag === "lo") onChange([Math.min(val, value[1] - 5), value[1]]); else onChange([value[0], Math.max(val, value[0] + 5)]); }
    function up() { setDrag(null); }
    window.addEventListener("pointermove", move); window.addEventListener("pointerup", up);
    return function() { window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", up); };
  }, [drag, value, min, max, onChange]);
  var thumb = function(i) { return { position:"absolute", left:pct(value[i])+"%", top:"50%", transform:"translate(-50%,-50%)", width:18, height:18, borderRadius:"50%", background:"#fff", border:"3px solid "+C.navy, cursor:"grab", touchAction:"none", boxShadow:"0 1px 4px rgba(0,0,0,0.15)", zIndex:i+1 }; };
  return (
    <div style={{ padding:"6px 0" }}>
      <div ref={ref} style={{ position:"relative", height:5, background:C.border, borderRadius:3 }}>
        <div style={{ position:"absolute", left:pct(value[0])+"%", right:(100-pct(value[1]))+"%", height:"100%", background:C.navy, borderRadius:3 }} />
        <div onPointerDown={function(e) { e.preventDefault(); setDrag("lo"); }} style={thumb(0)} />
        <div onPointerDown={function(e) { e.preventDefault(); setDrag("hi"); }} style={thumb(1)} />
      </div>
      <div style={{ display:"flex", justifyContent:"space-between", marginTop:4, fontSize:10, color:C.muted, fontWeight:600 }}><span>{value[0]} sqft</span><span>{value[1]} sqft</span></div>
    </div>
  );
}

export default function BookingEngine() {
  var _w = useState(typeof window !== "undefined" ? window.innerWidth : 375); var width = _w[0]; var setWidth = _w[1];
  useEffect(function() { function h() { setWidth(window.innerWidth); } window.addEventListener("resize", h); return function() { window.removeEventListener("resize", h); }; }, []);
  var isMobile = width < 768;
  var isTablet = width >= 768 && width < 1024;

  var _sf = useState("All"); var sizeFilter = _sf[0]; var setSizeFilter = _sf[1];
  var _sr = useState([0, 300]); var sqftRange = _sr[0]; var setSqftRange = _sr[1];
  var _ac = useState([]); var activeClimates = _ac[0]; var setActiveClimates = _ac[1];
  var _ad = useState(null); var activeDiscount = _ad[0]; var setActiveDiscount = _ad[1];
  var _cc = useState(""); var couponCode = _cc[0]; var setCouponCode = _cc[1];
  var _cr = useState(null); var couponResult = _cr[0]; var setCouponResult = _cr[1];
  var _sm = useState("selection"); var smartMode = _sm[0]; var setSmartMode = _sm[1];
  var _sfl = useState(false); var showFilters = _sfl[0]; var setShowFilters = _sfl[1];
  var _sp = useState(false); var showPromos = _sp[0]; var setShowPromos = _sp[1];
  var _sl = useState(false); var showLocation = _sl[0]; var setShowLocation = _sl[1];

  var facilityKey = "angMoKio";
  var facility = FACILITIES[facilityKey];

  useEffect(function() {
    var b = SIZES.find(function(s) { return s.label === sizeFilter; });
    if (b && sizeFilter !== "All") setSqftRange(b.range); else setSqftRange([0, 300]);
  }, [sizeFilter]);

  function toggleClimate(id) { setActiveClimates(function(p) { return p.includes(id) ? p.filter(function(c) { return c !== id; }) : p.concat([id]); }); }
  function toggleDiscount(d) { setActiveDiscount(function(p) { return p && p.id === d.id ? null : d; }); }
  function applyCoupon() { var code = couponCode.trim().toUpperCase(); setCouponResult(VALID_COUPONS[code] !== undefined ? { valid:true, pct:VALID_COUPONS[code], code:code } : { valid:false }); }
  function clearCoupon() { setCouponCode(""); setCouponResult(null); }

  var calcPrice = useCallback(function(base) {
    var p = base;
    if (activeDiscount && activeDiscount.type === "percent") p = base * (1 - activeDiscount.pct);
    if (couponResult && couponResult.valid) p = p * (1 - couponResult.pct);
    return p;
  }, [activeDiscount, couponResult]);

  var hasDiscount = activeDiscount || (couponResult && couponResult.valid);
  var isFirstFree = activeDiscount && activeDiscount.type === "first_free";

  var filteredUnits = useMemo(function() {
    var list = UNITS.slice();
    var isNearby = smartMode === "nearby" || smartMode === "nearby_best";
    var isBest = smartMode === "best_price" || smartMode === "nearby_best";
    if (isNearby) list = list.filter(function(u) { return u.facility !== facilityKey; });
    else list = list.filter(function(u) { return u.facility === facilityKey; });
    if (!isBest) {
      list = list.filter(function(u) { return u.sqft >= sqftRange[0] && u.sqft <= sqftRange[1]; });
      if (activeClimates.length) list = list.filter(function(u) { return activeClimates.includes(u.climate); });
    }
    if (isBest) list.sort(function(a, b) { return calcPrice(a.price) - calcPrice(b.price); });
    return list;
  }, [sizeFilter, sqftRange, activeClimates, smartMode, calcPrice]);

  var filterCount = (sizeFilter !== "All" ? 1 : 0) + activeClimates.length;
  var card = { background:C.card, borderRadius:C.radius, padding:isMobile ? 14 : 20, boxShadow:C.shadow };

  var css = "@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800;900&display=swap');*{box-sizing:border-box;margin:0;padding:0}::-webkit-scrollbar{height:0;width:0}.cs{display:flex;gap:6px;overflow-x:auto;padding-bottom:2px;scrollbar-width:none}.cs::-webkit-scrollbar{display:none}.fi{animation:fi .25s ease}@keyframes fi{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}.uc{transition:all .2s ease}@media(hover:hover){.uc:hover{box-shadow:" + C.shadowHover + ";transform:translateY(-2px)}}.uc:active{transform:scale(.98)}";

  return (
    <div style={{ minHeight:"100vh", background:C.bg, fontFamily:font, color:C.navy }}>
      <style>{css}</style>

      {/* HERO */}
      <section style={{ position:"relative", height:isMobile ? 180 : 230, overflow:"hidden", background:"linear-gradient(135deg," + C.navy + " 0%, #2d3f63 100%)" }}>
        <div style={{ position:"absolute", inset:0, opacity:0.04, backgroundImage:"repeating-linear-gradient(90deg,#fff 0,#fff 1px,transparent 1px,transparent 36px),repeating-linear-gradient(0deg,#fff 0,#fff 1px,transparent 1px,transparent 36px)" }} />
        <div style={{ position:"absolute", inset:0, background:"linear-gradient(100deg,rgba(27,42,74,0.95) 0%,rgba(27,42,74,0.5) 65%,transparent 100%)", display:"flex", flexDirection:"column", justifyContent:"center", padding:isMobile ? "0 20px" : "0 48px" }}>
          <div style={{ fontSize:9, fontWeight:600, letterSpacing:2.5, textTransform:"uppercase", color:"rgba(255,255,255,0.4)", marginBottom:5 }}>{facility.name} Facility</div>
          <h1 style={{ fontSize:isMobile ? 28 : 38, fontWeight:900, color:"#fff", lineHeight:1.1 }}>Find a <span style={{ color:C.red }}>Storage</span></h1>
          <p style={{ fontSize:isMobile ? 12 : 14, color:"rgba(255,255,255,0.6)", marginTop:7, maxWidth:340 }}>Secure, professionally managed units across Singapore.</p>
        </div>
        <div style={{ position:"absolute", bottom:-1, left:0, right:0, height:18, background:C.bg, borderRadius:"18px 18px 0 0" }} />
      </section>

      {/* STICKY FILTER BAR */}
      <nav style={{ position:"sticky", top:0, zIndex:50, background:"rgba(255,255,255,0.96)", backdropFilter:"blur(12px)", borderBottom:"1px solid " + C.border }}>
        {isMobile ? (
          <div>
            <div style={{ display:"flex", alignItems:"center", gap:6, padding:"8px 14px" }}>
              <div className="cs" style={{ flex:1 }}>
                {SIZES.map(function(s) { return (
                  <button key={s.label} onClick={function() { setSizeFilter(s.label); }} style={{ padding:"5px 12px", borderRadius:999, border:"none", fontSize:11, fontWeight:700, fontFamily:font, cursor:"pointer", whiteSpace:"nowrap", flexShrink:0, background:sizeFilter === s.label ? C.navy : "#EDEEF2", color:sizeFilter === s.label ? "#fff" : C.muted }}>{s.label}</button>
                ); })}
              </div>
              <button onClick={function() { setShowFilters(!showFilters); }} style={{ position:"relative", width:36, height:36, borderRadius:8, border:"1.5px solid " + (filterCount > 0 ? C.navy : C.border), background:filterCount > 0 ? C.navy + "06" : "#fff", cursor:"pointer", display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0 }}>
                <Filter size={14} color={C.navy} />
                {filterCount > 0 && <span style={{ position:"absolute", top:-3, right:-3, width:14, height:14, borderRadius:"50%", background:C.red, color:"#fff", fontSize:8, fontWeight:800, display:"flex", alignItems:"center", justifyContent:"center" }}>{filterCount}</span>}
              </button>
            </div>
            {showFilters && (
              <div className="fi" style={{ padding:"0 14px 12px", borderTop:"1px solid " + C.border }}>
                <div style={{ marginTop:10, marginBottom:8 }}>
                  <div style={{ fontSize:10, fontWeight:700, color:C.muted, marginBottom:4, letterSpacing:0.5 }}>SIZE RANGE</div>
                  <RangeSlider min={0} max={300} value={sqftRange} onChange={setSqftRange} />
                </div>
                <div>
                  <div style={{ fontSize:10, fontWeight:700, color:C.muted, marginBottom:4, letterSpacing:0.5 }}>CLIMATE TYPE</div>
                  <div className="cs">
                    {CLIMATES.map(function(ct) { var Icon = ct.icon; var active = activeClimates.includes(ct.id); return (
                      <button key={ct.id} onClick={function() { toggleClimate(ct.id); }} style={{ display:"flex", alignItems:"center", gap:4, padding:"5px 9px", borderRadius:7, border:"1.5px solid " + (active ? C.navy : C.border), background:active ? C.navy + "08" : "#fff", fontSize:10, fontWeight:600, fontFamily:font, cursor:"pointer", color:active ? C.navy : C.muted, whiteSpace:"nowrap", flexShrink:0 }}><Icon size={11} /> {ct.short}</button>
                    ); })}
                  </div>
                </div>
              </div>
            )}
          </div>
        ) : (
          <div style={{ maxWidth:1280, margin:"0 auto", display:"flex", alignItems:"center", gap:14, height:60, padding:"0 24px", overflowX:"auto" }} className="cs">
            <div style={{ display:"flex", background:"#EDEEF2", padding:3, borderRadius:999, gap:2, flexShrink:0 }}>
              {SIZES.map(function(s) { return (
                <button key={s.label} onClick={function() { setSizeFilter(s.label); }} style={{ padding:"5px 11px", borderRadius:999, border:"none", fontSize:11, fontWeight:700, fontFamily:font, cursor:"pointer", whiteSpace:"nowrap", background:sizeFilter === s.label ? C.navy : "transparent", color:sizeFilter === s.label ? "#fff" : C.muted }}>{s.label}</button>
              ); })}
            </div>
            <div style={{ width:1, height:26, background:C.border, flexShrink:0 }} />
            <div style={{ width:140, flexShrink:0 }}><RangeSlider min={0} max={300} value={sqftRange} onChange={setSqftRange} /></div>
            <div style={{ width:1, height:26, background:C.border, flexShrink:0 }} />
            <div className="cs">
              {CLIMATES.map(function(ct) { var Icon = ct.icon; var active = activeClimates.includes(ct.id); return (
                <button key={ct.id} onClick={function() { toggleClimate(ct.id); }} style={{ display:"flex", alignItems:"center", gap:4, padding:"5px 9px", borderRadius:7, border:"1.5px solid " + (active ? C.navy : C.border), background:active ? C.navy + "08" : "#fff", fontSize:10, fontWeight:600, fontFamily:font, cursor:"pointer", color:active ? C.navy : C.muted, whiteSpace:"nowrap", flexShrink:0 }}><Icon size={11} /> {ct.short}</button>
              ); })}
            </div>
          </div>
        )}
      </nav>

      {/* MAIN */}
      <main style={{ maxWidth:1280, margin:"0 auto", padding:isMobile ? "14px 14px 100px" : "22px 24px 50px", display:"flex", flexDirection:isMobile ? "column" : "row", gap:isMobile ? 14 : 22 }}>

        {/* LEFT */}
        <aside style={{ width:isMobile ? "100%" : "33%", minWidth:isMobile ? "auto" : 270, flexShrink:0, display:"flex", flexDirection:"column", gap:14 }}>
          {/* Location */}
          <div style={card}>
            {isMobile ? (
              <div>
                <button onClick={function() { setShowLocation(!showLocation); }} style={{ display:"flex", alignItems:"center", justifyContent:"space-between", width:"100%", background:"none", border:"none", cursor:"pointer", fontFamily:font, padding:0 }}>
                  <div style={{ display:"flex", alignItems:"center", gap:7 }}>
                    <div style={{ width:30, height:30, borderRadius:8, background:C.red + "12", display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0 }}><MapPin size={14} color={C.red} /></div>
                    <div style={{ textAlign:"left" }}><div style={{ fontSize:14, fontWeight:800, color:C.navy }}>{facility.name}</div><div style={{ fontSize:10, color:C.muted, marginTop:1 }}>{facility.address.split(",")[0]}</div></div>
                  </div>
                  {showLocation ? <ChevronUp size={15} color={C.muted} /> : <ChevronDown size={15} color={C.muted} />}
                </button>
                {showLocation && (
                  <div className="fi" style={{ marginTop:12, fontSize:10, color:C.muted }}>
                    {[["Access", facility.hours, Clock], ["Office", facility.office, Clock], ["Phone", facility.phone, Phone]].map(function(item) { var l = item[0]; var v = item[1]; var Icon = item[2]; return (
                      <div key={l} style={{ display:"flex", justifyContent:"space-between", alignItems:"center", padding:"5px 0", borderBottom:"1px solid " + C.border }}><span style={{ display:"flex", alignItems:"center", gap:4 }}><Icon size={10} /> {l}</span><span style={{ fontWeight:700, color:C.navy }}>{v}</span></div>
                    ); })}
                  </div>
                )}
              </div>
            ) : (
              <div>
                <div style={{ display:"flex", alignItems:"center", gap:6, marginBottom:4 }}><MapPin size={15} color={C.red} /><h3 style={{ fontSize:15, fontWeight:800 }}>{facility.name}</h3></div>
                <p style={{ fontSize:11, color:C.muted, lineHeight:1.4, marginBottom:10 }}>{facility.address}</p>
                <div style={{ fontSize:11, color:C.muted }}>
                  {[["Access", facility.hours, Clock], ["Office", facility.office, Clock], ["Phone", facility.phone, Phone]].map(function(item) { var l = item[0]; var v = item[1]; var Icon = item[2]; return (
                    <div key={l} style={{ display:"flex", justifyContent:"space-between", alignItems:"center", padding:"5px 0", borderBottom:"1px solid " + C.border }}><span style={{ display:"flex", alignItems:"center", gap:4 }}><Icon size={11} /> {l}</span><span style={{ fontWeight:700, color:C.navy }}>{v}</span></div>
                  ); })}
                </div>
                <button style={{ width:"100%", marginTop:10, padding:"8px 0", borderRadius:8, border:"1.5px solid " + C.navy, background:"transparent", color:C.navy, fontSize:11, fontWeight:700, fontFamily:font, cursor:"pointer", display:"flex", alignItems:"center", justifyContent:"center", gap:5 }}><Navigation size={12} /> Get Directions</button>
              </div>
            )}
          </div>

          {/* Promos */}
          <div style={Object.assign({}, card, { borderTop:"3px solid " + C.red, padding:isMobile ? "12px 14px" : "18px 20px" })}>
            <button onClick={function() { if (isMobile) setShowPromos(!showPromos); }} style={{ display:"flex", alignItems:"center", justifyContent:"space-between", width:"100%", background:"none", border:"none", cursor:isMobile ? "pointer" : "default", fontFamily:font, padding:0 }}>
              <div style={{ display:"flex", alignItems:"center", gap:6 }}><Tag size={13} color={C.red} /><span style={{ fontSize:13, fontWeight:800, color:C.navy }}>Promotions & Discounts</span></div>
              {isMobile && (showPromos ? <ChevronUp size={15} color={C.muted} /> : <ChevronDown size={15} color={C.muted} />)}
            </button>
            {(!isMobile || showPromos) && (
              <div className="fi" style={{ marginTop:10 }}>
                <div style={{ display:"flex", flexDirection:"column", gap:7 }}>
                  {DISCOUNTS.map(function(d) { var active = activeDiscount && activeDiscount.id === d.id; return (
                    <button key={d.id} onClick={function() { toggleDiscount(d); }} style={{ textAlign:"left", padding:"9px 11px", borderRadius:9, border:"1.5px solid " + (active ? C.navy : C.border), background:active ? C.navy + "06" : "#fff", cursor:"pointer", fontFamily:font, position:"relative" }}>
                      {d.badge && <span style={{ position:"absolute", top:-6, right:8, background:C.red, color:"#fff", fontSize:8, fontWeight:800, padding:"1px 5px", borderRadius:4, letterSpacing:0.4 }}>{d.badge}</span>}
                      <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
                        <div><div style={{ fontSize:12, fontWeight:700, color:C.navy }}>{d.name}</div><div style={{ fontSize:10, color:C.muted, marginTop:1 }}>{d.desc}</div></div>
                        <div style={{ width:15, height:15, borderRadius:"50%", flexShrink:0, marginLeft:8, border:"2px solid " + (active ? C.navy : C.border), background:active ? C.navy : "#fff", display:"flex", alignItems:"center", justifyContent:"center" }}>{active && <div style={{ width:5, height:5, borderRadius:"50%", background:"#fff" }} />}</div>
                      </div>
                    </button>
                  ); })}
                </div>
                <div style={{ marginTop:12, paddingTop:12, borderTop:"1px solid " + C.border }}>
                  <div style={{ display:"flex", gap:6 }}>
                    <div style={{ flex:1, position:"relative" }}>
                      <Ticket size={12} style={{ position:"absolute", left:9, top:"50%", transform:"translateY(-50%)", color:C.muted }} />
                      <input type="text" placeholder="Promo code" value={couponCode} onChange={function(e) { setCouponCode(e.target.value.toUpperCase()); setCouponResult(null); }} onKeyDown={function(e) { if (e.key === "Enter") applyCoupon(); }} style={{ width:"100%", boxSizing:"border-box", padding:"8px 8px 8px 28px", borderRadius:8, border:"1.5px solid " + (couponResult && !couponResult.valid ? C.red : C.border), fontSize:11, fontWeight:600, fontFamily:font, outline:"none", background:"#FAFBFC", color:C.navy }} />
                    </div>
                    <button onClick={applyCoupon} style={{ padding:"0 14px", borderRadius:8, border:"none", background:C.navy, color:"#fff", fontSize:11, fontWeight:700, fontFamily:font, cursor:"pointer" }}>Apply</button>
                  </div>
                  {couponResult && couponResult.valid && <div style={{ display:"flex", alignItems:"center", gap:4, marginTop:5, fontSize:10, color:C.green, fontWeight:700 }}><CheckCircle2 size={11} /> {couponResult.code} — {(couponResult.pct * 100).toFixed(0)}% off<button onClick={clearCoupon} style={{ marginLeft:"auto", background:"none", border:"none", cursor:"pointer", color:C.muted, padding:0 }}><X size={11} /></button></div>}
                  {couponResult && !couponResult.valid && <div style={{ display:"flex", alignItems:"center", gap:4, marginTop:5, fontSize:10, color:C.red, fontWeight:700 }}><AlertCircle size={11} /> Invalid code. Try SAVE10 or EXTRA15.</div>}
                </div>
              </div>
            )}
          </div>
        </aside>

        {/* RIGHT */}
        <section style={{ flex:1, minWidth:0 }}>
          {/* Smart mode bar */}
          <div style={{ display:"flex", gap:0, background:C.navy, borderRadius:10, padding:3, marginBottom:14 }}>
            {MODES.map(function(m) { var Icon = m.icon; var active = smartMode === m.id; return (
              <button key={m.id} onClick={function() { setSmartMode(m.id); }} style={{ flex:1, display:"flex", alignItems:"center", justifyContent:"center", gap:4, padding:isMobile ? "9px 2px" : "9px 10px", borderRadius:8, border:"none", background:active ? "#fff" : "transparent", color:active ? C.navy : "rgba(255,255,255,0.5)", fontSize:isMobile ? 10 : 11, fontWeight:700, fontFamily:font, cursor:"pointer", transition:"all 0.2s", whiteSpace:"nowrap" }}><Icon size={isMobile ? 12 : 14} />{isMobile ? m.mLabel : m.label}</button>
            ); })}
          </div>

          <div style={{ marginBottom:12 }}><h2 style={{ fontSize:isMobile ? 17 : 19, fontWeight:800 }}>Available Units</h2><p style={{ fontSize:11, color:C.muted, marginTop:2 }}>{filteredUnits.length} unit{filteredUnits.length !== 1 ? "s" : ""} found</p></div>

          <div style={{ display:"grid", gridTemplateColumns:isMobile ? "repeat(2, 1fr)" : isTablet ? "repeat(2, 1fr)" : "repeat(3, 1fr)", gap:isMobile ? 10 : 12 }}>
            {filteredUnits.map(function(unit) {
              var dp = calcPrice(unit.price);
              var showStrike = hasDiscount && dp < unit.price;
              var fac = FACILITIES[unit.facility];
              var isOther = unit.facility !== facilityKey;
              return (
                <div key={unit.id} className="uc" style={{ background:C.card, borderRadius:C.radius, padding:isMobile ? 12 : 14, boxShadow:C.shadow, cursor:"pointer", display:"flex", flexDirection:"column" }}>
                  {isOther && <div style={{ fontSize:9, fontWeight:700, color:C.red, background:C.red + "10", padding:"2px 6px", borderRadius:4, marginBottom:5, display:"inline-flex", alignItems:"center", gap:3, alignSelf:"flex-start" }}><MapPin size={8} /> {fac && fac.name}</div>}
                  <div style={{ display:"flex", alignItems:"center", gap:6, marginBottom:7 }}>
                    <span style={{ width:24, height:24, borderRadius:"50%", background:C.navy, color:"#fff", fontSize:10, fontWeight:800, display:"flex", alignItems:"center", justifyContent:"center" }}>{unit.label}</span>
                    <span style={{ fontSize:11, fontWeight:600, color:C.muted }}>Locker {unit.id}</span>
                  </div>
                  <div style={{ background:"#F8F9FB", borderRadius:8, padding:"8px 10px", marginBottom:7 }}>
                    <div style={{ fontSize:17, fontWeight:800, letterSpacing:-0.5 }}>{unit.dims}</div>
                    <div style={{ fontSize:9, color:C.muted, fontWeight:600, textTransform:"uppercase", letterSpacing:0.7, marginTop:1 }}>Est: {unit.sqft} sqft</div>
                  </div>
                  <div style={{ display:"flex", flexWrap:"wrap", gap:3, marginBottom:8, flex:1 }}>
                    {unit.tags.map(function(t) { return <span key={t} style={{ padding:"2px 6px", background:"#F0F1F4", color:C.muted, borderRadius:4, fontSize:9, fontWeight:600 }}>{t}</span>; })}
                    <span style={{ padding:"2px 6px", background:"#EBF4FF", color:"#3b82f6", borderRadius:4, fontSize:9, fontWeight:600 }}>{unit.climate}</span>
                  </div>
                  <div style={{ borderTop:"1px solid " + C.border, paddingTop:8, display:"flex", alignItems:"center", justifyContent:"space-between" }}>
                    <div>
                      {isFirstFree && <div style={{ fontSize:10, fontWeight:800, color:C.green, marginBottom:1 }}>1st Month FREE</div>}
                      {showStrike && !isFirstFree && <div style={{ fontSize:10, color:C.muted, textDecoration:"line-through" }}>SGD {unit.price.toFixed(2)}</div>}
                      <div style={{ display:"flex", alignItems:"baseline", gap:3 }}>
                        <span style={{ fontSize:15, fontWeight:900, color:(showStrike || isFirstFree) ? C.red : C.navy }}>SGD {isFirstFree ? unit.price.toFixed(2) : dp.toFixed(2)}</span>
                        <span style={{ fontSize:10, color:C.muted, fontWeight:500 }}>/ mo</span>
                      </div>
                      {isFirstFree && <div style={{ fontSize:8, color:C.muted }}>from 2nd month</div>}
                    </div>
                    <div style={{ width:32, height:32, borderRadius:8, background:C.navy, color:"#fff", display:"flex", alignItems:"center", justifyContent:"center", flexShrink:0 }}><ChevronRight size={15} /></div>
                  </div>
                </div>
              );
            })}
          </div>

          {filteredUnits.length === 0 && (
            <div style={{ textAlign:"center", padding:"44px 14px", background:"#fff", borderRadius:C.radius, border:"2px dashed " + C.border }}>
              <Search size={28} color={C.border} />
              <p style={{ fontSize:12, fontWeight:600, color:C.muted, marginTop:8 }}>No units match these filters.</p>
              <button onClick={function() { setSizeFilter("All"); setActiveClimates([]); setSqftRange([0, 300]); }} style={{ marginTop:6, background:"none", border:"none", color:C.navy, fontWeight:700, fontSize:11, cursor:"pointer", textDecoration:"underline", fontFamily:font }}>Clear all filters</button>
            </div>
          )}
        </section>
      </main>

      {/* FABs */}
      <div style={{ position:"fixed", bottom:isMobile ? 14 : 22, right:isMobile ? 14 : 22, display:"flex", flexDirection:"column", gap:8, zIndex:60 }}>
        <button style={{ width:42, height:42, borderRadius:"50%", border:"1px solid " + C.border, background:"#fff", color:C.navy, cursor:"pointer", display:"flex", alignItems:"center", justifyContent:"center", boxShadow:"0 3px 12px rgba(0,0,0,0.1)" }}><MessageCircle size={17} /></button>
        <button style={{ width:42, height:42, borderRadius:"50%", border:"none", background:C.red, color:"#fff", cursor:"pointer", display:"flex", alignItems:"center", justifyContent:"center", boxShadow:"0 3px 12px rgba(230,57,70,0.3)" }}><Calendar size={17} /></button>
      </div>
    </div>
  );
}
