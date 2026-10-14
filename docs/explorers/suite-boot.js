/* Runs in <head> before first paint: choose the theme and define K(), the colour
   mapper every animation uses. The animation code names its colours in the dark
   palette it was drawn in; K() returns them unchanged in the dark theme and maps
   them to a light-paper equivalent otherwise:
   - neutrals and dark tints: lightness is mirrored (surfaces become paper, text becomes ink),
     keeping a little of the hue;
   - pure white stays white (it is text or a halo on coloured chips);
   - accents: hue and saturation are kept and lightness is solved for a fixed relative
     luminance, so every accent has the same contrast on paper and with white text. */
(function(){
 var t=null;try{t=localStorage.getItem('ariabc_theme')}catch(e){}
 if(t!=='light'&&t!=='dark')t=(window.matchMedia&&matchMedia('(prefers-color-scheme: dark)').matches)?'dark':'light';
 document.documentElement.setAttribute('data-theme',t);window.SUITE_THEME=t;
 var cache={};
 function hsl(r,g,b){var mx=Math.max(r,g,b),mn=Math.min(r,g,b),l=(mx+mn)/2,h=0,s=0,d=mx-mn;
  if(d){s=l>.5?d/(2-mx-mn):d/(mx+mn);h=mx===r?(g-b)/d+(g<b?6:0):mx===g?(b-r)/d+2:(r-g)/d+4;h/=6}return[h,s,l,d]}
 function rgb(h,s,l){if(!s)return[l,l,l];var q=l<.5?l*(1+s):l+s-l*s,p=2*l-q;
  function f(t){if(t<0)t+=1;if(t>1)t-=1;return t<1/6?p+(q-p)*6*t:t<1/2?q:t<2/3?p+(q-p)*(2/3-t)*6:p}return[f(h+1/3),f(h),f(h-1/3)]}
 function lum(c){var a=c.map(function(v){return v<=.03928?v/12.92:Math.pow((v+.055)/1.055,2.4)});return .2126*a[0]+.7152*a[1]+.0722*a[2]}
 function hex(c){return'#'+c.map(function(v){return('0'+Math.round(Math.max(0,Math.min(1,v))*255).toString(16)).slice(-2)}).join('')}
 function light(src){var m=src.slice(1);if(m.length===3)m=m.replace(/./g,'$&$&');var a=m.length===8?m.slice(6):'';
  var r=parseInt(m.slice(0,2),16)/255,g=parseInt(m.slice(2,4),16)/255,b=parseInt(m.slice(4,6),16)/255;var x=hsl(r,g,b),h=x[0],s=x[1],l=x[2],chroma=x[3],out;
  /* pure white is only ever text or a halo on coloured chips: keep it */
  if(chroma<.02&&l>.97)return'#ffffff'+a;
  if(chroma<.2){var l2=l<.5?.995-.5*l:Math.max(.11,1.09-l);out=rgb(h,Math.min(s,.5)*.55,l2)}
  else{var lo=.05,hi=.95,want=.15;for(var i=0;i<22;i++){var mid=(lo+hi)/2;if(lum(rgb(h,s,mid))>want)hi=mid;else lo=mid}out=rgb(h,Math.min(1,s*.92),(lo+hi)/2)}
  return hex(out)+a}
 window.K=function(c){if(t==='dark')return c;return cache[c]||(cache[c]=light(c))};
})();
